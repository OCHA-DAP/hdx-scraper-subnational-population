from os.path import join
from rasterio import open as r_open

import pytest
from hdx.api.configuration import Configuration
from hdx.utilities.path import temp_dir
from hdx.utilities.useragent import UserAgent
from population_raster import PopulationRaster


class TestPopulationRaster:
    mapbox_countries = ["LSO"]
    rendered_raster = r_open(join("tests", "fixtures", "lso_render.tif")).read()

    @pytest.fixture(scope="function")
    def configuration(self):
        UserAgent.set_global("test")
        Configuration._create(
            hdx_read_only=True,
            hdx_site="prod",
            project_config_yaml=join("config", "project_configuration.yml"),
        )
        return Configuration.read()

    @pytest.fixture(scope="function")
    def downloader(self):
        class Download:

            @staticmethod
            def urlretrieve(url, path):
                return join(
                    "tests", "fixtures", "lso_ppp_2020.tif"
                )

        return Download()

    def test_generate_mapbox_data(self, configuration):
        with temp_dir("TestPopulation", delete_on_success=True, delete_on_failure=False) as temp_folder:
            pop_rast = PopulationRaster("mapbox_auth", configuration["legend"], temp_folder)
            rendered_rasters = pop_rast.generate_mapbox_data(TestPopulationRaster.mapbox_countries)
            rendered_raster = r_open(rendered_rasters[TestPopulationRaster.mapbox_countries[0]]).read()
            assert (rendered_raster == TestPopulationRaster.rendered_raster).all()
