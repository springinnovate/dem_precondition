# build_catalog.py
import os
import pystac
import rasterio
from shapely.geometry import box, mapping
from pathlib import Path


def create_catalog(root_dir, catalog_id, catalog_description):
    catalog = pystac.Catalog(
        id=catalog_id,
        description=catalog_description
    )

    collection = pystac.Collection(
        id=f'{catalog_id}-collection',
        description="Pre-routed DEM slices aligned with HUC05s",
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent([[-180, -90, 180, 90]]),
            temporal=pystac.TemporalExtent([[None, None]])
        )
    )
    catalog.add_child(collection)

    root_path = Path(root_dir).resolve()

    for root, _, files in os.walk(root_path):
        for fname in files:
            if fname.lower().endswith((".tif", ".tiff")):
                full_path = Path(root) / fname
                with rasterio.open(full_path) as src:
                    bounds = src.bounds
                    geom = box(*bounds)
                    bbox = [bounds.left, bounds.bottom, bounds.right, bounds.top]

                item = pystac.Item(
                    id=full_path.stem,
                    geometry=mapping(geom),
                    bbox=bbox,
                    datetime=None,
                    properties={}
                )

                item.add_asset(
                    key="data",
                    asset=pystac.Asset(
                        href=str(full_path),
                        media_type=pystac.MediaType.COG
                    )
                )

                collection.add_item(item)

    catalog.normalize_and_save(
        root_href="stac-catalog", 
        catalog_type=pystac.CatalogType.SELF_CONTAINED)


if __name__ == "__main__":
    create_catalog(
        root_dir="ASTGTM_mfd_routed",
        catalog_id="astgtm-huc05",
        catalog_description='ASTGTM processed by HUC05 subwatersheds.')
