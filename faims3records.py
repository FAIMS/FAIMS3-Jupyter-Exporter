import csv
import datetime
import geopandas
import os
import re
import shutil
import sys
import textwrap
import unicodedata
import tqdm
from faims3couchdb import CouchDBHelper

from shapely.geometry import Point
import fiona
from pathlib import Path

fiona.supported_drivers['KML'] = 'rw'
#https://stackoverflow.com/a/52851541

def slugify(value, allow_unicode=True):
    """
    Remove characters that aren't alphanumerics, underscores, or hyphens.


    From https://stackoverflow.com/a/295466, taken from https://github.com/django/django/blob/master/django/utils/text.py
    Convert to ASCII if 'allow_unicode' is False. Convert spaces or repeated
    dashes to single dashes. Remove characters that aren't alphanumerics,
    underscores, or hyphens. Also strip leading and
    trailing whitespace, dashes, and underscores.
    """
    value = str(value)
    if allow_unicode:
        value = unicodedata.normalize('NFKC', value)
        placeholder = "..."
    else:
        value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('ascii')
    value = re.sub(r'[^\w\s-]', '', value)
    return re.sub(r'[-\s]+', '-', textwrap.shorten(value, width=200, placeholder=placeholder)).strip('-_')

class FAIMS3Record:
    def __init__(self, *, user, token, base_url, project_key):
        self.export_date = f"{datetime.date.today().isoformat()}"
        self.faims = CouchDBHelper(user=user
                          ,token=token
                          ,base_url=base_url
                          ,project_key=project_key
                          )
        

        self.records = self.faims.get_fetched_records()
        self.record_fieldnames = self.faims.record_fieldnames
        self.record_definitions = self.faims.record_definitions
        self.record_lookup_definitions = self.faims.record_fieldnames_odict
        self.field_metadata = self.faims.field_metadata
        self.records_with_points_gdf = None
        self.clean_project_name = slugify(self.faims.project_id)
        self.base_export_path = Path(f'FAIMS3_Export+{self.export_date}+{self.clean_project_name}')
        self.project_metadata = self.faims.project_metadata
        self.project_metadata_attachments = self.faims.project_metadata_attachments
        self.project_name = self.faims.project_id
        self.record_count = self.faims.record_count

    def parse_record_metdata(self, record_type):
        """
        Builds a theoretical record definition...
        """
        all_fields = self.field_metadata


    def get_fetched_geodataframe(self):
        if type(self.records_with_points_gdf) == None:
            return self.get_geodataframes_for_take_points()
        return self.records_with_points_gdf


    def get_geodataframes_for_take_points(self, crs="EPSG:4326"):
        """ Make geodataframes for all geospatial records containing a take point button"""

        records = self.records
        records_with_points_gdf = {}
        for recordtype in records:
            #print(recordtype)
            try:
                #TODO this will need to be generalised for multiple geospatial datatypes
                #     But it will wait until type is properly passed over to the data.
                latitude_key = [x for x in records[recordtype][0].keys() if 'latitude' in x].pop()
                longitude_key = [x for x in records[recordtype][0].keys() if 'longitude' in x].pop()                
                geo_list_for_df = []
                if latitude_key and longitude_key:
                    for record in records[recordtype]:
                        if longitude := record.get(longitude_key):
                            if latitutde := record.get(latitude_key):
                                geo_record = record | {'geometry': Point(longitude, latitutde)}
                                geo_list_for_df.append(geo_record)
                if geo_list_for_df:
                    records_with_points_gdf[recordtype] = geopandas.GeoDataFrame(geo_list_for_df, crs=crs)
            except IndexError:
                # No key found
                continue

        self.records_with_points_gdf = records_with_points_gdf
        return records_with_points_gdf


    def to_csv(self, records, fieldnames, basedir = None, filename = None):
        """
        Export a single CSV
        """

        with open(basedir / f"{filename}.csv", "w", newline='') as csvfile:
            csvwriter = csv.DictWriter(csvfile, fieldnames=fieldnames)
            csvwriter.writeheader()
            for row in records:
                csvwriter.writerow(row)

        return basedir / f"{filename}.csv"

    def to_csvs(self
               , basedir = None
               , basefilename = None
               , delimiter="+"):
        """
        Take a records object and turn it into CSVs. 

        This is avoiding using a geodataframe as we can't rely on geodata
        being present in every recordtype. Theoretically we could load into a
        normal dataframe, but that's overkill.
        
        """

        if basedir is None:
            basedir = self.base_export_path.joinpath("csv")
        if basefilename is None:
            basefilename = f"{self.export_date}+{self.clean_project_name}"
        
        basedir.mkdir(parents=True, exist_ok=True)

        records = self.records
        csvs = {}
        #print(csvs)
        for record_type in records:
            csvs[record_type]=({ 'record_type': record_type
                        , 'record_definition': self.record_definitions[record_type]
                        , 'field_mappings': self.record_fieldnames
                        , 'csv_file':self.to_csv(records=records[record_type]
                                           ,fieldnames=self.record_fieldnames[record_type]
                                           ,basedir=basedir
                                           ,filename=f"{basefilename}-{record_type}")})
        return csvs

            
            


    def to_shapefiles(self
                     , basefilename=None
                     , delimiter="+"):

        if basefilename is None:
            basefilename = f"{self.export_date}+{self.project_id}"
        
        records = self.records
        gdf = self.get_fetched_geodataframe()

        for recordtype in records:
            gdf[recordtype].to_file(f"{basefilename}{delimiter}{recordtype}.shp", driver="ESRI Shapefile")                            

    def to_geojson(self
                  , basefilename=None
                  , delimiter="+"):
        
        if basefilename is None:
            basefilename = f"{self.export_date}+{self.project_id}"

        records = self.get_fetched_records()
        gdf = self.get_fetched_geodataframe()


        for recordtype in records:
            gdf[recordtype].to_file(f"{basefilename}{delimiter}{recordtype}.geojson", driver='GeoJSON')                            

    def to_geopackage(self
                     , basefilename=None):
        
        if basefilename is None:
            basefilename = f"{self.export_date}+{self.project_id}"

        records = self.records
        gdf = self.get_fetched_geodataframe()
        
        for recordtype in records:
            gdf[recordtype].to_file(f"{basefilename}.gpkg", layer=recordtype, driver="GPKG")                            


    def write_all_geospatial_to_dirs(self
                                    , basedir = None):
        """
        Placeholder until we get geojson stuffs, but this should detect lat-long and dump a shapefile.
        """

        if basedir is None:
            basedir = self.base_export_path

        gdf = self.get_geodataframes_for_take_points()
        
        shapedir = basedir / "shapefile"
        geojsondir = basedir / "geojson"
        kmldir = basedir / "kml"

        shapedir.mkdir(parents=True, exist_ok=True)
        geojsondir.mkdir(parents=True, exist_ok=True)
        kmldir.mkdir(parents=True, exist_ok=True)

        for recordtype in gdf:
            gdf[recordtype].to_file( shapedir / f"{self.export_date}+{self.clean_project_name}+{recordtype}.shp", driver="ESRI Shapefile")
            gdf[recordtype].to_file( geojsondir / f"{self.export_date}+{self.clean_project_name}+{recordtype}.geojson", driver='GeoJSON')
            gdf[recordtype].to_file( basedir / f"{self.export_date}+{self.clean_project_name}+sqlite.gpkg", layer=recordtype, driver="GPKG")
            gdf[recordtype].to_file( kmldir / f"{self.export_date}+{self.clean_project_name}+{recordtype}.kml", driver='KML')

    def move_metadata_attachments_to_dir(self,
                                         basedir = None):
        if basedir is None:
            basedir = self.base_export_path

        if self.project_metadata_attachments:
            metadir = basedir / "metadata_attachments"

            metadir.mkdir(parents=True, exist_ok=True)

            for attachment, path in self.project_metadata_attachments.items():
                #print(attachment, path)
                shutil.move(path, metadir / attachment)

