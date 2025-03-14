# build_catalog.py
import json
import os
import pystac
import rasterio
from shapely.geometry import box, mapping
from pathlib import Path
from ecoshard import geoprocessing

from osgeo import gdal


def create_catalog(root_dir, catalog_id, catalog_description):
    catalog = pystac.Catalog(
        id=catalog_id,
        description=catalog_description
    )

    indexing_vector = gdal.OpenEx('data/global_lev05.gpkg')
    indexing_layer = indexing_vector.GetLayer()

    prefix_list = [
        ('filled_dem',
         'filled',
         'Depression filled dem.'),
        ('flow_dir_mfd',
         'flow_dir_mfd',
         'geoprocessing.routing.flow_dir_mfd routed dem.')]

    for collection_prefix, collection_id, collection_description in prefix_list:
        collection = pystac.Collection(
            id=f'{catalog_id}-{collection_prefix}',
            description=collection_description,
            extent=pystac.Extent(
                spatial=pystac.SpatialExtent([[-180, -90, 180, 90]]),
                temporal=pystac.TemporalExtent([[None, None]])
            )
        )
        catalog.add_child(collection)

        root_path = Path(root_dir).resolve()

        for current_dir_path, _, file_list in os.walk(root_path):
            for filename in file_list:
                if (filename.endswith('.tif') and
                        filename.startswith(collection_prefix)):
                    raster_path = current_dir_path / filename
                    raster_info = geoprocessing.get_raster_info(raster_path)

                    raster_stem = Path(raster_path).stem
                    fid = int(raster_stem.split('_'))[-1]
                    feature = indexing_layer.GetFeature(fid)
                    geom = feature.GetGeometryRef()
                    geom_json_text = geom.ExportToJson()
                    geojson_geom = json.loads(geom_json_text)

                    item = pystac.Item(
                        id=raster_stem,
                        geometry=geojson_geom,
                        bbox=raster_info['bounding_box'],
                        datetime=None,
                        properties={}
                    )

                    item.add_asset(
                        key="data",
                        asset=pystac.Asset(
                            href=raster_path,
                            media_type=pystac.MediaType.GEOTIFF
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
