import hashlib
import logging
import os
import pickle
import sys

from ecoshard import taskgraph
from ecoshard.geoprocessing import routing
from osgeo import gdal, ogr
import psutil
import rasterio
import richdem as rd

logging.basicConfig(
    level=logging.DEBUG,
    stream=sys.stdout,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s '
        '[%(funcName)s:%(lineno)d] %(message)s'))
LOGGER = logging.getLogger(__name__)
logging.getLogger('ecoshard').setLevel(logging.INFO)
logging.getLogger('rasterio').setLevel(logging.INFO)


def create_directory_hash(subwatershed_id):
    hash_str = hashlib.sha256(str(subwatershed_id).encode('utf-8')).hexdigest()
    return os.path.join(hash_str[:2], hash_str[2:4])


def get_layer_and_geom_name(gpkg_path):
    vector_ds = ogr.Open(gpkg_path)
    layer = vector_ds.GetLayer()  # since there's only one layer
    layer_name = layer.GetName()

    # Attempt to detect geometry column name from the layer definition
    layer_defn = layer.GetLayerDefn()
    geom_field_defn = layer_defn.GetGeomFieldDefn(0)
    geometry_column_name = geom_field_defn.GetName()
    if not geometry_column_name:
        # Fall back if geometry column name is empty
        geometry_column_name = 'geom'
    return layer_name, geometry_column_name


def extract_dem_for_subwatershed(
        dem_path, gpkg_path, layer_name, geom_name,
        subwatershed_fid, output_raster_path):
    LOGGER.info(f'extracting dem for {subwatershed_fid}')
    cutline_sql = (
        f'SELECT ST_MakeValid({geom_name}) AS {geom_name} '
        f'FROM "{layer_name}" WHERE FID = {subwatershed_fid}'
    )
    warp_options = gdal.WarpOptions(
        format='GTiff',
        dstNodata=-9999,
        cropToCutline=True,
        cutlineDSName=gpkg_path,
        cutlineSQL=cutline_sql
    )
    gdal.Warp(
        output_raster_path,
        dem_path,
        options=warp_options
    )
    LOGGER.info(f'done extracting dem for {subwatershed_fid}')


def fill_with_richdem(
        global_dem_path_band_tuple, global_vector_path, layer_name, geom_name,
        subwatershed_fid, filled_dem_raster_path):
    global_dem_path, band_index = global_dem_path_band_tuple
    dem_basename = os.path.basename(os.path.splitext(global_dem_path)[0])
    LOGGER.info(f'extracting dem for {subwatershed_fid}')
    cutline_sql = (
        f'SELECT ST_MakeValid({geom_name}) AS {geom_name} '
        f'FROM "{layer_name}" WHERE FID = {subwatershed_fid}'
    )
    warp_options = gdal.WarpOptions(
        format='GTiff',
        dstNodata=-9999,
        cropToCutline=True,
        cutlineDSName=global_vector_path,
        cutlineSQL=cutline_sql
    )
    intermediate_cut_dem_path = os.path.join(os.path.dirname(
        filled_dem_raster_path),
        f'{dem_basename}_cut_to_{subwatershed_fid}.tif')
    gdal.Warp(
        global_dem_path,
        intermediate_cut_dem_path,
        options=warp_options
    )
    LOGGER.info(f'done extracting dem for {subwatershed_fid}')

    # Read the input DEM as a NumPy array using rasterio
    LOGGER.info(f'loading {intermediate_cut_dem_path} with rasterio')
    with rasterio.open(intermediate_cut_dem_path) as src:
        dem_array = src.read(band_index)
        profile = src.profile
        no_data = profile.get('nodata')
        transform = src.transform
        crs = src.crs

    original_dtype = profile['dtype']
    LOGGER.debug(f'creating rdarray from {intermediate_cut_dem_path}')
    rd_dem = rd.rdarray(dem_array, no_data=no_data)
    LOGGER.debug(f'filling depressionsfrom {intermediate_cut_dem_path}')
    rd_dem.geotransform = transform
    rd_filled = rd.FillDepressions(rd_dem, epsilon=False)
    profile.update(
        dtype=original_dtype,
        nodata=no_data,
        compress='lzw',
        count=1,
        transform=transform,
        crs=crs,
    )
    LOGGER.debug(f'writing filled dem to {filled_dem_raster_path}')
    with rasterio.open(filled_dem_raster_path, 'w', **profile) as dst:
        dst.write(rd_filled.astype(original_dtype), 1)

    try:
        LOGGER.info(f'cleaning up intermeidate {intermediate_cut_dem_path}')
        os.remove(intermediate_cut_dem_path)
    except Exception:
        LOGGER.warning(f'could not delete {intermediate_cut_dem_path}')


def process_subwatershed(
        task_graph, global_dem_path, gpkg_path,
        layer_name, geom_name, subwatershed_fid, workspace_dir):
    # Adjust file paths as needed

    filled_dem_raster_path = os.path.join(
        workspace_dir, f'filled_dem_{subwatershed_fid}.tif')
    flow_dir_path = os.path.join(
        workspace_dir, f'flow_dir_mfd_{subwatershed_fid}.tif')

    fill_pits_task = task_graph.add_task(
        func=fill_with_richdem,
        args=((global_dem_path, 1), filled_dem_raster_path, workspace_dir),
        target_path_list=[filled_dem_raster_path],
        task_name=f'fill_pits_subwatershed_{subwatershed_fid}')

    _ = task_graph.add_task(
        func=routing.flow_dir_mfd,
        args=((filled_dem_raster_path, 1), flow_dir_path),
        kwargs={'working_dir': workspace_dir},
        dependent_task_list=[fill_pits_task],
        target_path_list=[flow_dir_path],
        task_name=f'flow_dir_subwatershed_{subwatershed_fid}')
    return flow_dir_path


def get_fid_list(gpkg_path):
    vector = ogr.Open(gpkg_path)
    layer = vector.GetLayer()
    fid_list = [feature.GetFID() for feature in layer]
    layer = None
    vector = None
    return fid_list


def main():
    global_dem_path = './data/astgtm_compressed.tif'
    gpkg_path = './data/global_lev05.gpkg'
    workspace_dir = 'dem_pre_route_workspace'
    gpkg_basename = os.path.basename(os.path.splitext(gpkg_path)[0])
    n_cpus = psutil.cpu_count(logical=False)
    os.makedirs(workspace_dir, exist_ok=True)
    task_graph = taskgraph.TaskGraph(workspace_dir, n_cpus, 5.0)

    fid_list = get_fid_list(gpkg_path)
    index_dict = {
        'source_dem': global_dem_path,
        'source_subwatershed': gpkg_path,
        'subwatershed_routing_index': {}
    }

    layer_name, geom_name = get_layer_and_geom_name(gpkg_path)

    for index, subwatershed_fid in enumerate(fid_list):
        hash_subdir = create_directory_hash(subwatershed_fid)
        subwatershed_tag = f'{gpkg_basename}_{subwatershed_fid}'
        local_workspace_dir = os.path.join(
            workspace_dir, hash_subdir, subwatershed_tag)
        os.makedirs(local_workspace_dir, exist_ok=True)

        local_mfd_flow_path = process_subwatershed(
            task_graph, global_dem_path, gpkg_path,
            layer_name, geom_name,
            subwatershed_fid, local_workspace_dir)
        rel_local_mfd_flow_path = os.path.relpath(
            local_mfd_flow_path, workspace_dir)
        index_dict['subwatershed_routing_index'][subwatershed_fid] = rel_local_mfd_flow_path

    task_graph.join()
    task_graph.close()

    index_path = os.path.join(workspace_dir, 'index.pkl')
    with open(index_path, 'wb') as f:
        pickle.dump(index_dict, f)

    with open(index_path, 'rb') as f:
        new_index = pickle.loads(f.read())
    LOGGER.info(
        f'all done, processed {len(new_index["subwatershed_routing_index"])} dems')


if __name__ == '__main__':
    main()
