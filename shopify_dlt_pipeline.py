# shopify_pipeline.py
import logging
import dlt
from dlt.common import pendulum
from typing import List, Tuple
import time
from shopify_dlt import shopify_source, TAnyDateTime, shopify_partner_query

from shopify_extras import (
    load_pages,
    load_pages_metafields,
    load_collections_metafields,
    load_blogs,
    load_articles,
    load_inventory_levels_gql,
    load_products_metafields,
    load_b2b_companies,
)

# Logging config, DLT by default doesn't provide much in the way of logging.

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    force=True,
)
logger = logging.getLogger(__name__)


# Helper to run a loader function and log timing

def run_loader(name, fn, pipeline):
    logger.info(f"➡️ Starting loader: {name}")
    start = time.time()
    try:
        result = fn(pipeline)
        elapsed = round(time.time() - start, 2)
        logger.info(f"✅ Loader {name} complete in {elapsed}s")
        return result
    except Exception as e:
        elapsed = round(time.time() - start, 2)
        logger.exception(f"❌ Loader {name} failed after {elapsed}s")
        return None


# Core pipeline logic
def load_all_resources(resources: List[str], start_date: TAnyDateTime) -> None:
    logger.info("Starting load_all_resources")
    logger.info(f"Resources requested: {resources}")
    logger.info(f"Start date: {start_date}")

    pipeline = dlt.pipeline(
        pipeline_name="shopify_local",
        destination="postgres",
        dataset_name="shopify_dlt_data",
    )

    try:
        logger.info("Initializing Shopify source...")
        source = shopify_source(start_date=start_date).with_resources(*resources)

        # Core Shopify load (orders, products, customers, etc.)
        load_info = pipeline.run(source)
        logger.info("✅ Core pipeline run complete.")
        logger.info(f"Load info summary:\n{load_info}")

        # Run extra loaders
        run_loader("pages", load_pages, pipeline)
        run_loader("pages_metafields", load_pages_metafields, pipeline)
        run_loader("collections_metafields", load_collections_metafields, pipeline)
        # run_loader("products_metafields", load_products_metafields, pipeline)
        run_loader("blogs", load_blogs, pipeline)
        run_loader("articles", load_articles, pipeline)
        run_loader("inventory_levels_gql", load_inventory_levels_gql, pipeline)
        run_loader("b2b_companies", load_b2b_companies, pipeline)

        logger.info("✅ All custom Shopify resources loaded successfully.")
        logger.info("Data stored in: shopify.duckdb")

    except Exception:
        logger.exception("❌ Pipeline failed with error")
        raise


# Backloading function, this backfills the data in weekly chunks

def incremental_load_with_backloading() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="shopify_local",
        destination="postgres",
        dataset_name="shopify_dlt_data",
    )

    min_start_date = current_start_date = pendulum.datetime(2025, 10, 1)
    max_end_date = pendulum.now()

    ranges: List[Tuple[pendulum.DateTime, pendulum.DateTime]] = []
    while current_start_date < max_end_date:
        end_date = min(current_start_date.add(weeks=1), max_end_date)
        ranges.append((current_start_date, end_date))
        current_start_date = end_date

    logger.info(f"Starting backfill with {len(ranges)} weekly chunks")

    for idx, (start_date, end_date) in enumerate(ranges, start=1):
        logger.info(f"🧩 Chunk {idx}/{len(ranges)}: {start_date} → {end_date}")

        # Core Shopify load
        data = shopify_source(
            start_date=start_date, end_date=end_date, created_at_min=min_start_date
        ).with_resources("orders", "customers", "products")

        try:
            load_info = pipeline.run(data)
            logger.info(f"✅ Core chunk {idx} complete: {load_info}")

            # Include supplemental loaders per chunk
            run_loader("pages", load_pages, pipeline)
            run_loader("pages_metafields", load_pages_metafields, pipeline)
            run_loader("collections_metafields", load_collections_metafields, pipeline)
            # run_loader("products_metafields", load_products_metafields, pipeline)
            run_loader("blogs", load_blogs, pipeline)
            run_loader("articles", load_articles, pipeline)
            run_loader("b2b_companies", load_b2b_companies, pipeline)
            run_loader("b2b_company_contacts", load_b2b_company_contacts, pipeline)
            run_loader("b2b_contact_details", load_b2b_contact_details, pipeline)
            run_loader("inventory_levels_gql", load_inventory_levels_gql, pipeline)

            logger.info(f"✅ Supplemental loaders complete for chunk {idx}")

        except Exception:
            logger.exception(f"❌ Failed on chunk {idx} ({start_date} → {end_date})")
            break

    # After all chunks, run final incremental sync
    logger.info("Switching to incremental load from latest backfill point...")
    load_info = pipeline.run(
        shopify_source(
            start_date=max_end_date, created_at_min=min_start_date
        ).with_resources("orders", "customers", "products")
    )
    logger.info(f"✅ Incremental load complete: {load_info}")


# Partner API, this is added by DLT by default to the pipeline logic. Currently unused but worth keeping in the file for future reference

def load_partner_api_transactions() -> None:
    """Load transactions from the Shopify Partner API."""
    pipeline = dlt.pipeline(
        pipeline_name="shopify_partner",
        destination="postgres",
        dataset_name="shopify_partner_data",
    )

    query = """query Transactions($after: String, first: 100) {
        transactions(after: $after) {
            edges { cursor node { id } }
        }
    }"""

    resource = shopify_partner_query(
        query,
        data_items_path="data.transactions.edges[*].node",
        pagination_cursor_path="data.transactions.edges[-1].cursor",
        pagination_variable_name="after",
    )

    load_info = pipeline.run(resource)
    print(load_info)


# Initialisation of functions

if __name__ == "__main__":
    resources = ["products", "orders", "customers"]
    load_all_resources(resources, start_date="2025-10-10")
    # incremental_load_with_backloading()
    # load_partner_api_transactions()
