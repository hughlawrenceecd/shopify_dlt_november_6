import dlt
import requests
import time
import logging

def get_base_shop_domain() -> str:
    """
    Returns the shop domain stripped of protocol and trailing slashes.
    E.g. "https://staging-orderly.myshopify.com" -> "staging-orderly.myshopify.com"
    """
    shop_url = dlt.config.get("sources.shopify_dlt.shop_url") or ""
    return shop_url.replace("https://", "").replace("http://", "").strip("/")


def load_inventory_levels_gql(pipeline: dlt.Pipeline) -> None:
    """Loads inventory levels for the shop’s single location (Head Office)."""
    import requests
    import logging

    try:
        shop_domain = get_base_shop_domain()
        access_token = dlt.config.get("sources.shopify_dlt.private_app_password")
        if not shop_domain or not access_token:
            logging.warning("⚠️ Missing Shopify credentials; skipping inventory_levels_gql.")
            return

        gql_url = f"https://{shop_domain}/admin/api/2024-01/graphql.json"
        headers = {
            "X-Shopify-Access-Token": access_token,
            "Content-Type": "application/json",
        }

        # Step 1 — Get the one and only location dynamically
        loc_query = """
        query {
          locations(first: 1) {
            edges {
              node {
                id
                name
              }
            }
          }
        }
        """
        loc_resp = requests.post(gql_url, headers=headers, json={"query": loc_query}, timeout=30)
        loc_resp.raise_for_status()
        loc_data = loc_resp.json()
        edges = (loc_data.get("data") or {}).get("locations", {}).get("edges", [])

        if not edges:
            logging.error("❌ No locations found — check read_locations scope.")
            return

        head_office = edges[0]["node"]
        head_office_gid = head_office["id"]
        head_office_name = head_office["name"]
        logging.info(f"🏬 Using location: {head_office_name} ({head_office_gid})")

        # Step 2 — Query inventory levels for that location
        query = """
        query GetInventoryLevels($locationId: ID!, $first: Int!, $after: String) {
          location(id: $locationId) {
            inventoryLevels(first: $first, after: $after) {
              edges {
                node {
                  id
                  quantities(names: ["available", "incoming", "committed", "damaged", "on_hand"]) {
                    name
                    quantity
                  }
                  item { id sku }
                }
              }
              pageInfo { hasNextPage endCursor }
            }
          }
        }
        """

        @dlt.resource(write_disposition="replace", name="inventory_levels")
        def inventory_levels_resource():
            after = None
            total = 0

            while True:
                resp = requests.post(
                    gql_url,
                    headers=headers,
                    json={"query": query, "variables": {"locationId": head_office_gid, "first": 100, "after": after}},
                    timeout=60,
                )
                resp.raise_for_status()
                data = resp.json().get("data", {}).get("location", {})
                if not data:
                    logging.warning("⚠️ No location data in response — skipping batch.")
                    break

                inv_levels = data.get("inventoryLevels", {})
                edges = inv_levels.get("edges", [])
                if not edges:
                    break

                for edge in edges:
                    node = edge.get("node")
                    if node:
                        node["location_id"] = head_office_gid
                        node["location_name"] = head_office_name
                        total += 1
                        yield node

                page_info = inv_levels.get("pageInfo", {})
                if not page_info.get("hasNextPage"):
                    break
                after = page_info.get("endCursor")

            logging.info(f"✅ Finished loading {total} inventory levels from {head_office_name}.")

        pipeline.run(inventory_levels_resource())

    except Exception as e:
        logging.exception(f"❌ Failed to load inventory levels: {e}")

def load_pages(pipeline: dlt.Pipeline) -> None:
    try:
        shop_domain = get_base_shop_domain()
        access_token = dlt.config.get("sources.shopify_dlt.private_app_password")
        if not shop_domain or not access_token:
            return

        gql_url = f"https://{shop_domain}/admin/api/2024-01/graphql.json"
        headers = {"X-Shopify-Access-Token": access_token, "Content-Type": "application/json"}

        query = """
        query GetPages($first: Int!, $after: String) {
          pages(first: $first, after: $after) {
            edges { node { id title handle createdAt updatedAt } }
            pageInfo { hasNextPage endCursor }
          }
        }
        """

        @dlt.resource(write_disposition="replace", name="pages")
        def pages_resource():
            after = None
            total = 0

            while True:
                resp = requests.post(
                    gql_url,
                    headers=headers,
                    json={"query": query, "variables": {"first": 100, "after": after}},
                )
                resp.raise_for_status()
                data = resp.json()["data"]["pages"]

                batch_count = len(data["edges"])
                total += batch_count

                for edge in data["edges"]:
                    yield edge["node"]

                if not data["pageInfo"]["hasNextPage"]:
                    break
                after = data["pageInfo"]["endCursor"]

        shop_url = dlt.config.get("sources.shopify_dlt.shop_url")
        access_token = dlt.config.get("sources.shopify_dlt.private_app_password")
        if not shop_url or not access_token:
            return

        gql_url = f"https://{shop_url.replace('https://','').replace('http://','').strip('/')}/admin/api/2024-01/graphql.json"
        headers = {"X-Shopify-Access-Token": access_token, "Content-Type": "application/json"}

        query = """
        query GetPages($first: Int!, $after: String) {
          pages(first: $first, after: $after) {
            edges {
              node {
                id
                title
                handle
                createdAt
                updatedAt
              }
            }
            pageInfo {
              hasNextPage
              endCursor
            }
          }
        }
        """

        @dlt.resource(write_disposition="replace", name="pages")
        def pages_resource():
            after = None
            total = 0

            while True:
                resp = requests.post(
                    gql_url,
                    headers=headers,
                    json={"query": query, "variables": {"first": 100, "after": after}},
                )
                resp.raise_for_status()
                data = resp.json()["data"]["pages"]

                for edge in data["edges"]:
                    total += 1
                    yield edge["node"]

                print(f"📄 Loaded {len(data['edges'])} pages (total {total})")

                if not data["pageInfo"]["hasNextPage"]:
                    break
                after = data["pageInfo"]["endCursor"]

            print(f"✅ Finished loading {total} pages")

        pipeline.run(pages_resource())

    except Exception as e:
        print(f"❌ Failed to load pages: {e}")


def load_pages_metafields(pipeline: dlt.Pipeline) -> None:
    try:
        shop_domain = get_base_shop_domain()
        access_token = dlt.config.get("sources.shopify_dlt.private_app_password")
        if not shop_domain or not access_token:
            return

        base_url = f"https://{shop_domain}/admin/api/2024-01"
        headers = {"X-Shopify-Access-Token": access_token, "Content-Type": "application/json"}

        pages_url = f"{base_url}/pages.json?limit=250"
        page_ids = []
        while pages_url:
            resp = requests.get(pages_url, headers=headers)
            resp.raise_for_status()
            data = resp.json()["pages"]
            page_ids.extend([p["id"] for p in data])

            link_header = resp.headers.get("Link", "")
            next_url = None
            if 'rel="next"' in link_header:
                next_url = link_header.split(";")[0].strip("<>")
            pages_url = next_url

        @dlt.resource(write_disposition="replace", name="pages_metafields")
        def pages_metafields_resource():
            for pid in page_ids:
                url = f"{base_url}/pages/{pid}/metafields.json"
                resp = requests.get(url, headers=headers)
                resp.raise_for_status()
                for mf in resp.json()["metafields"]:
                    mf["page_id"] = pid
                    yield mf

        pipeline.run(pages_metafields_resource())

    except Exception as e:
        print(f"❌ Failed to load pages metafields: {e}")


def load_collections_metafields(pipeline: dlt.Pipeline) -> None:
    try:
        shop_domain = get_base_shop_domain()
        access_token = dlt.config.get("sources.shopify_dlt.private_app_password")
        if not shop_domain or not access_token:
            return

        base_url = f"https://{shop_domain}/admin/api/2024-01"
        headers = {"X-Shopify-Access-Token": access_token, "Content-Type": "application/json"}

        url = f"{base_url}/custom_collections.json?limit=250"
        collection_ids = []
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        collections = resp.json()["custom_collections"]
        collection_ids.extend([c["id"] for c in collections])

        @dlt.resource(write_disposition="replace", name="collections_metafields")
        def collections_metafields_resource():
            for cid in collection_ids:
                resp = requests.get(f"{base_url}/collections/{cid}/metafields.json", headers=headers)
                resp.raise_for_status()
                for mf in resp.json()["metafields"]:
                    mf["collection_id"] = cid
                    yield mf

        pipeline.run(collections_metafields_resource())

    except Exception as e:
        print(f"❌ Failed to load collections metafields: {e}")


def load_blogs(pipeline: dlt.Pipeline) -> None:
    try:
        shop_domain = get_base_shop_domain()
        access_token = dlt.config.get("sources.shopify_dlt.private_app_password")
        if not shop_domain or not access_token:
            return

        gql_url = f"https://{shop_domain}/admin/api/2024-01/graphql.json"
        headers = {"X-Shopify-Access-Token": access_token, "Content-Type": "application/json"}

        query = """
        query GetBlogs($first: Int!, $after: String) {
          blogs(first: $first, after: $after) {
            edges {
              node { id title handle createdAt updatedAt }
            }
            pageInfo { hasNextPage endCursor }
          }
        }
        """

        @dlt.resource(write_disposition="replace", name="blogs")
        def blogs_resource():
            after = None
            total = 0
            while True:
                resp = requests.post(
                    gql_url,
                    headers=headers,
                    json={"query": query, "variables": {"first": 100, "after": after}},
                )
                resp.raise_for_status()
                data = resp.json()["data"]["blogs"]
                for edge in data["edges"]:
                    total += 1
                    yield edge["node"]
                if not data["pageInfo"]["hasNextPage"]:
                    break
                after = data["pageInfo"]["endCursor"]
            print(f"✅ Loaded {total} blogs")

        pipeline.run(blogs_resource())

    except Exception as e:
        print(f"❌ Failed to load blogs: {e}")


def load_articles(pipeline: dlt.Pipeline) -> None:
    try:
        shop_domain = get_base_shop_domain()
        access_token = dlt.config.get("sources.shopify_dlt.private_app_password")
        if not shop_domain or not access_token:
            return

        gql_url = f"https://{shop_domain}/admin/api/2024-01/graphql.json"
        headers = {"X-Shopify-Access-Token": access_token, "Content-Type": "application/json"}

        query = """
        query GetArticles($first: Int!, $after: String) {
          articles(first: $first, after: $after) {
            edges {
              node { id title handle createdAt updatedAt }
            }
            pageInfo { hasNextPage endCursor }
          }
        }
        """

        @dlt.resource(write_disposition="replace", name="articles")
        def articles_resource():
            after = None
            total = 0
            while True:
                resp = requests.post(
                    gql_url,
                    headers=headers,
                    json={"query": query, "variables": {"first": 100, "after": after}},
                )
                resp.raise_for_status()
                data = resp.json()["data"]["articles"]
                for edge in data["edges"]:
                    total += 1
                    yield edge["node"]
                if not data["pageInfo"]["hasNextPage"]:
                    break
                after = data["pageInfo"]["endCursor"]
            print(f"✅ Loaded {total} articles")

        pipeline.run(articles_resource())

    except Exception as e:
        print(f"❌ Failed to load articles: {e}")

def load_products_metafields(pipeline: dlt.Pipeline) -> None:
    """Loads product metafields with progress tracking and defensive timeouts."""
    try:
        shop_domain = get_base_shop_domain()
        access_token = dlt.config.get("sources.shopify_dlt.private_app_password")
        if not shop_domain or not access_token:
            logging.warning("⚠️ Missing Shopify credentials; skipping product_metafields.")
            return

        base_url = f"https://{shop_domain}/admin/api/2024-01"
        headers = {"X-Shopify-Access-Token": access_token, "Content-Type": "application/json"}

        # Step 1: Collect all product IDs
        products_url = f"{base_url}/products.json?limit=250"
        product_ids = []
        total_pages = 0

        while products_url:
            total_pages += 1
            resp = requests.get(products_url, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json().get("products", [])
            product_ids.extend([p["id"] for p in data])

            link_header = resp.headers.get("Link", "")
            next_url = None
            if 'rel="next"' in link_header:
                next_url = link_header.split(";")[0].strip("<>")
            products_url = next_url

        if not product_ids:
            logging.warning("⚠️ No products found; skipping metafield load.")
            return

        # Step 2: Fetch metafields per product
        start_time = time.time()
        total_metafields = 0
        total_products = len(product_ids)
        last_log_time = start_time

        @dlt.resource(write_disposition="replace", name="products_metafields")
        def products_metafields_resource():
            nonlocal total_metafields
            for i, product_id in enumerate(product_ids, start=1):
                try:
                    url = f"{base_url}/products/{product_id}/metafields.json"
                    resp = requests.get(url, headers=headers, timeout=20)
                    resp.raise_for_status()
                    metafields = resp.json().get("metafields", [])

                    for mf in metafields:
                        mf["product_id"] = product_id
                        total_metafields += 1
                        yield mf

                except requests.exceptions.RequestException as re:
                    logging.warning(f"⚠️ Request error for product {product_id}: {re}")
                    time.sleep(1)
                    continue

        pipeline.run(products_metafields_resource())

        total_time = round(time.time() - start_time, 2)
        logging.info(
            f"✅ Finished loading {total_metafields} metafields for {total_products} products in {total_time}s."
        )

    except Exception as e:
        logging.exception(f"❌ Failed to load product metafields: {e}")
        shop_domain = get_base_shop_domain()
        access_token = dlt.config.get("sources.shopify_dlt.private_app_password")
        if not shop_domain or not access_token:
            return

        base_url = f"https://{shop_domain}/admin/api/2024-01"
        headers = {"X-Shopify-Access-Token": access_token, "Content-Type": "application/json"}

        # First, get all product IDs
        products_url = f"{base_url}/products.json?limit=250"
        product_ids = []
        
        while products_url:
            resp = requests.get(products_url, headers=headers)
            resp.raise_for_status()
            data = resp.json()["products"]
            product_ids.extend([p["id"] for p in data])

            # Handle pagination
            link_header = resp.headers.get("Link", "")
            next_url = None
            if 'rel="next"' in link_header:
                next_url = link_header.split(";")[0].strip("<>")
            products_url = next_url

        @dlt.resource(write_disposition="replace", name="products_metafields")
        def products_metafields_resource():
            total_metafields = 0
            for i, product_id in enumerate(product_ids):
                    url = f"{base_url}/products/{product_id}/metafields.json"
                    resp = requests.get(url, headers=headers)
                    resp.raise_for_status()
                    
                    metafields = resp.json().get("metafields", [])
                    for mf in metafields:
                        mf["product_id"] = product_id
                        total_metafields += 1
                        yield mf
                    

        pipeline.run(products_metafields_resource())

def load_b2b_companies(pipeline: dlt.Pipeline) -> None:
    """
    Loads B2B companies and their main contact details.
    Produces exactly two tables:
      - b2b_companies
      - b2b_main_contacts
    """


    try:
        shop_domain = get_base_shop_domain()
        access_token = dlt.config.get("sources.shopify_dlt.private_app_password")
        if not shop_domain or not access_token:
            logging.warning("⚠️ Missing Shopify credentials; skipping b2b_companies.")
            return

        gql_url = f"https://{shop_domain}/admin/api/2025-10/graphql.json"
        headers = {
            "X-Shopify-Access-Token": access_token,
            "Content-Type": "application/json",
        }

        # ✅ One unified query — includes mainContact and full customer info
        query = """
        query GetCompanies($first: Int!, $after: String) {
          companies(first: $first, after: $after) {
            edges {
              node {
                id
                name
                externalId
                note
                createdAt
                updatedAt

                mainContact {
                  id
                  customer {
                    id
                    firstName
                    lastName
                    createdAt
                    defaultEmailAddress { emailAddress }
                    amountSpent { amount currencyCode }
                  }
                }
              }
            }
            pageInfo { hasNextPage endCursor }
          }
        }
        """

        # ✅ Pagination + flattening
        def fetch_all_companies():
            after = None
            all_rows = []

            while True:
                r = requests.post(
                    gql_url,
                    headers=headers,
                    json={"query": query, "variables": {"first": 100, "after": after}},
                    timeout=30,
                )
                r.raise_for_status()
                pl = r.json()

                if "errors" in pl:
                    logging.error(f"❌ Shopify B2B API error: {pl['errors']}")
                    return []

                block = pl["data"]["companies"]
                edges = block.get("edges", [])
                if not edges:
                    break

                for e in edges:
                    all_rows.append(e["node"])

                if not block["pageInfo"]["hasNextPage"]:
                    break

                after = block["pageInfo"]["endCursor"]

            return all_rows

        companies = fetch_all_companies()
        logging.info(f"✅ Loaded {len(companies)} B2B companies")

        # ------------------------------
        # ✅ TABLE 1: b2b_companies
        # ------------------------------
        @dlt.resource(write_disposition="replace", name="b2b_companies")
        def companies_resource():
            for c in companies:
                yield {
                    "id": c.get("id"),
                    "name": c.get("name"),
                    "externalId": c.get("externalId"),
                    "note": c.get("note"),
                    "createdAt": c.get("createdAt"),
                    "updatedAt": c.get("updatedAt"),
                }

        # ------------------------------
        # ✅ TABLE 2: b2b_main_contacts
        # ------------------------------
        @dlt.resource(write_disposition="replace", name="b2b_main_contacts")
        def main_contacts_resource():
            for c in companies:
                mc = c.get("mainContact")
                if not mc:
                    continue

                cust = mc.get("customer") or {}
                email_obj = cust.get("defaultEmailAddress") or {}

                yield {
                    "contact_id": mc.get("id"),
                    "company_id": c.get("id"),
                    "customer_id": cust.get("id"),
                    "first_name": cust.get("firstName"),
                    "last_name": cust.get("lastName"),
                    "email": email_obj.get("emailAddress"),
                    "customer_created_at": cust.get("createdAt"),
                    "amount_spent_amount": (cust.get("amountSpent") or {}).get("amount"),
                    "amount_spent_currency": (cust.get("amountSpent") or {}).get("currencyCode"),
                }

        # ✅ Run both resources
        pipeline.run(companies_resource())
        pipeline.run(main_contacts_resource())

    except Exception as e:
        logging.exception(f"❌ Failed to load B2B companies: {e}")

def load_b2b_company_locations(pipeline: dlt.Pipeline) -> None:
    """
    Loads B2B company locations with flattened billing & shipping addresses.
    Produces a single DLT table: b2b_company_locations
    """
    import requests
    import logging

    try:
        shop_domain = get_base_shop_domain()
        access_token = dlt.config.get("sources.shopify_dlt.private_app_password")

        if not shop_domain or not access_token:
            logging.warning("⚠️ Missing Shopify credentials; skipping b2b_company_locations.")
            return

        gql_url = f"https://{shop_domain}/admin/api/2025-10/graphql.json"
        headers = {
            "X-Shopify-Access-Token": access_token,
            "Content-Type": "application/json",
        }

        # ✅ Query: only shallow fields + addresses
        query = """
        query GetCompanyLocations($first: Int!, $after: String) {
          companyLocations(first: $first, after: $after) {
            edges {
              node {
                id
                name
                externalId
                note
                phone
                createdAt
                updatedAt
                currency

                company { id }

                billingAddress {
                  address1
                  address2
                  city
                  province
                  country
                  zip
                }

                shippingAddress {
                  address1
                  address2
                  city
                  province
                  country
                  zip
                }

                ordersCount { count }
                catalogsCount { count }
                totalSpent { amount currencyCode }
              }
            }
            pageInfo { hasNextPage endCursor }
          }
        }
        """

        # --------------------------
        # ✅ Pagination helper
        # --------------------------
        def fetch_all_locations():
            all_rows = []
            after = None

            while True:
                r = requests.post(
                    gql_url,
                    headers=headers,
                    json={"query": query, "variables": {"first": 100, "after": after}},
                    timeout=30,
                )
                r.raise_for_status()
                pl = r.json()

                if "errors" in pl:
                    logging.error(f"❌ Shopify B2B API error: {pl['errors']}")
                    return []

                block = pl["data"]["companyLocations"]
                edges = block.get("edges", [])
                if not edges:
                    break

                for e in edges:
                    all_rows.append(e["node"])

                if not block["pageInfo"]["hasNextPage"]:
                    break

                after = block["pageInfo"]["endCursor"]

            return all_rows

        locations = fetch_all_locations()
        logging.info(f"✅ Loaded {len(locations)} B2B company locations")

        # --------------------------
        # ✅ Flatten + yield as 1 table
        # --------------------------
        @dlt.resource(write_disposition="replace", name="b2b_company_locations")
        def locations_resource():
            for loc in locations:
                billing = loc.get("billingAddress") or {}
                shipping = loc.get("shippingAddress") or {}

                yield {
                    # ---- Core fields ----
                    "id": loc.get("id"),
                    "company_id": (loc.get("company") or {}).get("id"),
                    "name": loc.get("name"),
                    "external_id": loc.get("externalId"),
                    "note": loc.get("note"),
                    "phone": loc.get("phone"),
                    "created_at": loc.get("createdAt"),
                    "updated_at": loc.get("updatedAt"),
                    "currency": loc.get("currency"),

                    # ---- Counts ----
                    "orders_count": (loc.get("ordersCount") or {}).get("count"),
                    "catalogs_count": (loc.get("catalogsCount") or {}).get("count"),

                    # ---- Money ----
                    "total_spent_amount": (loc.get("totalSpent") or {}).get("amount"),
                    "total_spent_currency": (loc.get("totalSpent") or {}).get("currencyCode"),

                    # ---- Billing address ----
                    "billing_address1": billing.get("address1"),
                    "billing_address2": billing.get("address2"),
                    "billing_city": billing.get("city"),
                    "billing_province": billing.get("province"),
                    "billing_country": billing.get("country"),
                    "billing_zip": billing.get("zip"),

                    # ---- Shipping address ----
                    "shipping_address1": shipping.get("address1"),
                    "shipping_address2": shipping.get("address2"),
                    "shipping_city": shipping.get("city"),
                    "shipping_province": shipping.get("province"),
                    "shipping_country": shipping.get("country"),
                    "shipping_zip": shipping.get("zip"),
                }

        pipeline.run(locations_resource())

    except Exception as e:
        logging.exception(f"❌ Failed to load B2B company locations: {e}")
