import logging
import string

from mapbox import Uploader
from numpy import zeros
from os.path import join
from random import choices
from rasterio import open as r_open
from rasterio.dtypes import uint8
from rasterio.enums import Resampling
from time import sleep
from urllib.request import urlretrieve

logger = logging.getLogger()


class PopulationRaster:
    def __init__(self, legend, temp_folder):
        self.temp_folder = temp_folder
        self.legend = legend
        self.rendered_rasters = dict()

    def generate_mapbox_data(self, countries):
        for iso in countries:
            ftp_url = f"ftp://ftp.worldpop.org.uk/GIS/Population/Global_2000_2020/2020/{iso.upper()}/{iso.lower()}_ppp_2020.tif"
            orig_raster = join(self.temp_folder, f"{iso.lower()}_ppp_2020.tif")
            try:
                path, _ = urlretrieve(ftp_url, orig_raster)
            except:
                logger.error(f"{iso}: Could not download population raster")
                continue

            open_raster = r_open(orig_raster)
            resample_raster = join(self.temp_folder, f"{iso}_resample.tif")
            render_raster = join(self.temp_folder, f"{iso.lower()}_render.tif")
            meta = open_raster.meta

            # Resample raster to decrease resolution
            with r_open(orig_raster) as src:
                scale_factor = 3000/src.width
                data = src.read(
                    out_shape=(src.count, int(src.height * scale_factor), int(src.width * scale_factor)),
                    resampling=Resampling.bilinear
                )
                transform = src.transform * src.transform.scale(
                    (src.width / data.shape[-1]),
                    (src.height / data.shape[-2])
                )
            meta.update({"height": data.shape[1],
                         "width": data.shape[2],
                         "transform": transform})
            with r_open(resample_raster, "w", **meta) as dst:
                dst.write(data)

            # Render as raster with red, green, blue, alpha bands
            color_bands = [zeros(shape=data.shape, dtype=uint8),
                           zeros(shape=data.shape, dtype=uint8),
                           zeros(shape=data.shape, dtype=uint8),
                           zeros(shape=data.shape, dtype=uint8)]
            scale = zeros(shape=data.shape)
            for color in self.legend:
                scale = (data - color["range"][0]) / (color["range"][1] - color["range"][0])
                color_bands[0][(scale >= 0) & (scale <= 1)] = color["color"][0][0] + (scale[(scale >= 0) & (scale <= 1)] * (color["color"][1][0] - color["color"][0][0])).astype(int)
                color_bands[1][(scale >= 0) & (scale <= 1)] = color["color"][0][1] + (scale[(scale >= 0) & (scale <= 1)] * (color["color"][1][1] - color["color"][0][1])).astype(int)
                color_bands[2][(scale >= 0) & (scale <= 1)] = color["color"][0][2] + (scale[(scale >= 0) & (scale <= 1)] * (color["color"][1][2] - color["color"][0][2])).astype(int)
            color_bands[3][data > -1] = 255
            meta.update({"count": 4,
                         "dtype": 'uint8',
                         "nodata": None})
            with r_open(render_raster, "w", **meta) as final:
                meta.update({"count": 1})
                for i, c in enumerate(color_bands, start=1):
                    color_raster = join(self.temp_folder, f"{iso}_color.tif")
                    with r_open(color_raster, "w", **meta) as dst:
                        dst.write(c)
                    with r_open(color_raster) as src:
                        final.write_band(i, src.read(1))
            self.rendered_rasters[iso] = render_raster

    def upload_to_mapbox(self, mapbox_auth):
        results = dict()
        service = Uploader(access_token=mapbox_auth)
        alphabet = string.ascii_lowercase + string.digits
        for country in self.rendered_rasters:
            mapid = f"humdata.{''.join(choices(alphabet, k=8))}"
            name = f"{country.lower()}_ppp_2020-{''.join(choices(alphabet, k=6))}"
            with open(self.rendered_rasters[country], 'rb') as src:
                upload_resp = service.upload(src, mapid, name=name)
            if upload_resp.status_code == 422:
                for i in range(5):
                    sleep(5)
                    with open(self.rendered_rasters[country], 'rb') as src:
                        upload_resp = service.upload(src, mapid, name=name)
                    if upload_resp.status_code != 422:
                        break
            if upload_resp.status_code == 422:
                logger.error(f"Could not upload {name}")
                return None
            results[country] = {"mapid": mapid, "name": name}
        return results
