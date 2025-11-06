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
    """Loads B2B companies + mainContact + contacts into separate DLT resources."""
    import requests
    import logging

    try:
        shop_url = dlt.config.get("sources.shopify_dlt.shop_url")
        access_token = dlt.config.get("sources.shopify_dlt.private_app_password")

        if not shop_url or not access_token:
            logging.warning("⚠️ Missing Shopify credentials: skipping B2B companies")
            return

        domain = shop_url.replace("https://", "").replace("http://", "").strip("/")
        gql_url = f"https://{domain}/admin/api/2025-10/graphql.json"

        headers = {
            "X-Shopify-Access-Token": access_token,
            "Content-Type": "application/json",
        }

        # ✅ IMPORTANT:
        # Contact does NOT have email/firstName/lastName
        # Contact is only a pointer to a Customer via ID
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
                }

                contacts(first: 50) {
                  edges {
                    node {
                      id
                    }
                  }
                }

                locationsCount { count }
                contactsCount  { count }
                totalSpent     { amount currencyCode }
              }
            }
            pageInfo {
              hasNextPage
              endCursor
            }
          }
        }
        """

        # ------------------------------------------------------------------
        # ✅ COMPANIES TABLE
        # ------------------------------------------------------------------
        @dlt.resource(
            write_disposition="replace",
            name="b2b_companies",
            columns={
                "note": {"data_type": "text"}  # Shopify returns null for many companies
            }
        )
        def companies_resource():
            after = None
            total = 0

            while True:
                resp = requests.post(
                    gql_url,
                    headers=headers,
                    json={"query": query, "variables": {"first": 50, "after": after}},
                    timeout=30,
                )
                resp.raise_for_status()
                payload = resp.json()

                if "errors" in payload:
                    logging.error(f"❌ Shopify B2B API error: {payload['errors']}")
                    return

                companies = payload["data"]["companies"]
                edges = companies["edges"]

                for edge in edges:
                    total += 1
                    yield edge["node"]

                if not companies["pageInfo"]["hasNextPage"]:
                    break

                after = companies["pageInfo"]["endCursor"]

            logging.info(f"✅ Loaded {total} B2B companies")

        # ------------------------------------------------------------------
        # ✅ MAIN CONTACTS TABLE
        # ------------------------------------------------------------------
        @dlt.resource(write_disposition="replace", name="b2b_company_main_contacts")
        def main_contacts_resource():
            for company in companies_resource():
                mc = company.get("mainContact")
                if mc:
                    mc["company_id"] = company["id"]
                    yield mc

        # ------------------------------------------------------------------
        # ✅ CONTACTS TABLE (list of contacts)
        # ------------------------------------------------------------------
        @dlt.resource(write_disposition="replace", name="b2b_company_contacts")
        def contacts_resource():
            for company in companies_resource():
                contacts = (
                    company.get("contacts", {})
                           .get("edges", [])
                )
                for edge in contacts:
                    node = edge.get("node")
                    if node:
                        node["company_id"] = company["id"]
                        yield node

        # ✅ Run all three
        pipeline.run(companies_resource())
        pipeline.run(main_contacts_resource())
        pipeline.run(contacts_resource())

    except Exception as e:
        logging.exception(f"❌ Failed to load B2B companies: {e}")
    """Loads B2B companies + mainContact + contacts into separate DLT resources."""
    try:
        shop_url = dlt.config.get("sources.shopify_dlt.shop_url")
        access_token = dlt.config.get("sources.shopify_dlt.private_app_password")
        if not shop_url or not access_token:
            logging.warning("⚠️ Missing Shopify credentials; skipping B2B companies.")
            return

        gql_url = f"https://{shop_url.replace('https://','').replace('http://','').strip('/')}/admin/api/2024-01/graphql.json"
        headers = {"X-Shopify-Access-Token": access_token, "Content-Type": "application/json"}

        # We request: companies + mainContact + contacts(first: 50)
        # Everything else stays unchanged
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
        }

        contacts(first: 50) {
          edges {
            node {
              id
            }
          }
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}

        """


        @dlt.resource(write_disposition="replace", name="b2b_companies")
        def companies_resource():
            after = None
            total = 0

            while True:
                resp = requests.post(
                    gql_url,
                    headers=headers,
                    json={"query": query, "variables": {"first": 50, "after": after}},
                    timeout=30,
                )
                resp.raise_for_status()

                payload = resp.json()
                if "errors" in payload:
                    logging.error(f"❌ Shopify B2B API error: {payload['errors']}")
                    return

                data = payload.get("data", {}).get("companies", {})
                edges = data.get("edges", [])

                for edge in edges:
                    node = edge.get("node")
                    if not node:
                        continue

                    total += 1
                    yield node

                page_info = data.get("pageInfo", {})
                if not page_info.get("hasNextPage"):
                    break
                after = page_info.get("endCursor")

            logging.info(f"✅ Loaded {total} B2B companies")

        @dlt.resource(write_disposition="replace", name="b2b_company_main_contacts")
        def main_contacts_resource(companies=companies_resource):
            for company in companies:
                mc = company.get("mainContact")
                if mc:
                    mc["company_id"] = company["id"]
                    yield mc

        @dlt.resource(write_disposition="replace", name="b2b_company_contacts")
        def contacts_resource(companies=companies_resource):
            for company in companies:
                contact_edges = (
                    company.get("contacts", {})
                           .get("edges", [])
                )
                for edge in contact_edges:
                    node = edge.get("node")
                    if node:
                        node["company_id"] = company["id"]
                        yield node

        # Run all three
        pipeline.run(companies_resource())
        pipeline.run(main_contacts_resource())
        pipeline.run(contacts_resource())

    except Exception as e:
        logging.exception(f"❌ Failed to load B2B companies: {e}")
    try:
        shop_url = dlt.config.get("sources.shopify_dlt.shop_url")
        access_token = dlt.config.get("sources.shopify_dlt.private_app_password")
        if not shop_url or not access_token:
            logging.warning("⚠️ Missing Shopify credentials; skipping b2b_companies.")
            return

        domain = shop_url.replace("https://","").replace("http://","").strip("/")
        # Use the newer version for B2B
        gql_url = f"https://{domain}/admin/api/2025-10/graphql.json"
        headers = {"X-Shopify-Access-Token": access_token, "Content-Type": "application/json"}

        query = """
        query GetCompanies($first: Int!, $after: String) {
          companies(first: $first, after: $after) {
            edges {
              node {
                id
                name
                createdAt
                updatedAt
                contactsCount { count }
                locationsCount { count }
                totalSpent { amount currencyCode }
                mainContact { id email firstName lastName }
              }
            }
            pageInfo { hasNextPage endCursor }
          }
        }
        """

        @dlt.resource(write_disposition="replace", name="b2b_companies")
        def companies_resource():
            after = None
            total = 0
            while True:
                resp = requests.post(
                    gql_url,
                    headers=headers,
                    json={"query": query, "variables": {"first": 50, "after": after}},
                    timeout=30,
                )
                resp.raise_for_status()
                json_resp = resp.json()

                # NEW: show Shopify GraphQL errors explicitly
                if "errors" in json_resp:
                    logging.error(f"❌ Shopify B2B API error: {json_resp['errors']}")
                    return

                companies_data = json_resp["data"]["companies"]
                edges = companies_data["edges"]
                if not edges:
                    break

                for edge in edges:
                    total += 1
                    yield edge["node"]

                if not companies_data["pageInfo"]["hasNextPage"]:
                    break
                after = companies_data["pageInfo"]["endCursor"]

            logging.info(f"✅ Loaded {total} B2B companies")

        pipeline.run(companies_resource())

    except Exception as e:
        logging.exception(f"❌ Failed to load B2B companies: {e}")
