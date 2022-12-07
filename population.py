import logging
import re
from pandas import concat, read_csv
from rasterstats import zonal_stats
from slugify import slugify

from hdx.data.dataset import Dataset
from hdx.location.country import Country
from hdx.utilities.downloader import DownloadError

logger = logging.getLogger()


class Population:
    def __init__(self, configuration, downloader, subnational_jsons, temp_folder):
        self.downloader = downloader
        self.boundaries = subnational_jsons
        self.temp_folder = temp_folder
        self.exceptions = {"dataset": configuration.get("dataset_exceptions", {}),
                           "resource": configuration.get("resource_exceptions", {})}
        self.headers = configuration["pcode_mappings"]
        self.skip = configuration.get("do_not_process", [])

    def run(self, countries):
        updated_countries = dict()
        for iso in countries:
            levels = list(set(self.boundaries["ADM_LEVEL"].loc[(self.boundaries["alpha_3"] == iso)]))
            for level in levels:
                if level not in updated_countries:
                    updated_countries[level] = list()
                logger.info(f"{iso}: Processing population at adm{level}")

                # find dataset and resource to use
                dataset = Dataset.read_from_hdx(self.exceptions["dataset"].get(iso, f"cod-ps-{iso.lower()}"))
                if not dataset:
                    logger.warning(f"{iso}: Could not find pop dataset")
                    continue
                resources = dataset.get_resources()
                resource_name = self.exceptions["resource"].get(iso, f"adm(in)?{level}")
                pop_resource = [r for r in resources if r.get_file_type() == "csv" and
                                bool(re.match(f".*{resource_name}.*", r["name"], re.IGNORECASE))]
                if len(pop_resource) == 0:
                    logger.error(f"{iso}: Could not find csv resource at adm{level}")
                    continue

                if not pop_resource:
                    dataset = Dataset.read_from_hdx(
                        f"worldpop-population-counts-for-{slugify(Country.get_country_name_from_iso3(iso))}"
                    )
                    resources = dataset.get_resources()
                    pop_resource = [r for r in resources if r.get_file_type() == "geotiff" and
                                    bool(re.match("(?<!\d)\d{4}_constrained", r["name"], re.IGNORECASE))]
                    if not pop_resource:
                        logger.warning(f"{iso}: Could not find any data at {level}")
                        continue

                    # download data
                    try:
                        _, pop_raster = pop_resource[0].download(folder=self.temp_folder)
                    except DownloadError:
                        logger.error(f"{iso}: Could not download geotiff")
                        continue

                    pop_stats = zonal_stats(
                        vectors=self.boundaries.loc[(self.boundaries["alpha_3"] == iso) &
                                                    (self.boundaries["ADM_LEVEL"] == level)],
                        raster=pop_raster,
                        stats="sum",
                        geojson_out=True,
                    )
                    for row in pop_stats:
                        pcode = row["properties"]["ADM_PCODE"]
                        pop = row["properties"]["sum"]
                        if pop:
                            pop = int(round(pop, 0))
                            self.boundaries.loc[self.boundaries["ADM_PCODE"] == pcode, "Population"] = pop
                    if iso not in updated_countries[level]:
                        updated_countries[level].append(iso)
                    continue

                if len(pop_resource) > 1:
                    yearmatches = [
                        re.findall("(?<!\d)\d{4}(?!\d)", r["name"], re.IGNORECASE)
                        for r in pop_resource
                    ]
                    yearmatches = sum(yearmatches, [])
                    if len(yearmatches) > 0:
                        yearmatches = [int(y) for y in yearmatches]
                    maxyear = [
                        r for r in pop_resource if str(max(yearmatches)) in r["name"]
                    ]
                    if len(maxyear) == 1:
                        pop_resource = maxyear

                if len(pop_resource) > 1:
                    logger.warning(f"{iso}: Found multiple resources, using first in list")

                headers, iterator = self.downloader.get_tabular_rows(
                    pop_resource[0]["url"], dict_form=True
                )

                pcode_header = None
                pop_header = None
                for header in headers:
                    if pcode_header and pop_header:
                        continue
                    if not pcode_header:
                        if header.upper() in [h.replace("#", level) for h in self.headers]:
                            pcode_header = header
                    if header.upper() == "T_TL":
                        pop_header = header

                if not pcode_header:
                    logger.error(f"{iso}: Could not find pcode header at {level}")
                    continue
                if not pop_header:
                    logger.error(f"{iso}: Could not find pop header at {level}")
                    continue

                for row in iterator:
                    pcode = row[pcode_header]
                    pop = row[pop_header]
                    if pcode not in list(self.boundaries["ADM_PCODE"]):
                        logger.warning(f"{iso}: Could not find unit {pcode} in boundaries at {level}")
                    else:
                        self.boundaries.loc[self.boundaries["ADM_PCODE"] == pcode, "Population"] = pop
                        if iso not in updated_countries[level]:
                            updated_countries[level].append(iso)

        return updated_countries

    def update_hdx_resource(self, dataset_name, updated_countries):
        dataset = Dataset.read_from_hdx(dataset_name)
        if not dataset:
            logger.error("Could not find overall pop dataset")
            return None, None

        resource = dataset.get_resources()[0]
        try:
            _, pop_data = resource.download(folder=self.temp_folder)
        except DownloadError:
            logger.error(f"Could not download population csv")
            return None, None
        pop_data = read_csv(pop_data)

        updated_data = self.boundaries.drop(columns="geometry")
        for level in updated_countries:
            pop_data.drop(pop_data[(pop_data["alpha_3"].isin(updated_countries[level])) &
                                   (pop_data["ADM_LEVEL"] == level)].index, inplace=True)
            pop_data = concat([pop_data,
                               updated_data.loc[(updated_data["alpha_3"].isin(updated_countries[level])) &
                                                (updated_data["ADM_LEVEL"] == level)]])

        pop_data.sort_values(by=["alpha_3", "ADM_LEVEL", "ADM_PCODE"], inplace=True)
        return pop_data, resource
