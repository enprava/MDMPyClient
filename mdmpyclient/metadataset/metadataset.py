import copy
import json
import logging
import sys
import subprocess

import pandas as pd
import requests
import yaml

from bs4 import BeautifulSoup

from selenium.webdriver.common.by import By
from selenium import webdriver

fmt = '[%(asctime)-15s] [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=fmt, level=logging.INFO, stream=sys.stdout)


class Metadataset:
    """ Clase que representa un metadataset del M&D Manager.

               Args:
                   session (:class:`requests.session.Session`): Sesión autenticada en la API.
                   configuracion (:class:`Diccionario`): Diccionario del que se obtienen algunos
                    parámetros necesarios como la url de la API. Debe ser inicializado a partir del
                    fichero de configuración configuracion/configuracion.yaml.
                   id (:class:`String`): Identificadordel metadataset
                   name (:class:`Diccionario`): Nombres del metadataset.
                   init_data (:class:`Boolean`): True para traer todos los datos del metadataset,
                    False para no traerlos. Por defecto toma el valor False.

               Attributes:
                   data (:obj:`Diccionario`): Diccionario con todos los datos del metadataset.
               """

    def __init__(self, session, configuracion, meta_id, names, init_data=False):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')

        self.session = session
        self.configuracion = configuracion
        self.id = meta_id
        self.names = names
        self.reports = self.get(init_data)

    def get(self, init_data):
        reports = {'id': [], 'code': [], 'published': []}
        if init_data:
            self.logger.info('Solicitando reportes del metadataset con id %s', self.id)
            try:
                response = self.session.get(
                    f'{self.configuracion["url_base"]}api/RM/getJsonMetadataset/{self.id}'
                    f'/?excludeReport=false&withAttributes=false')
                response_data = response.json()['data']['metadataSets'][0]['reports']
                response.raise_for_status()
            except KeyError:
                self.logger.error('Ha ocurrido un error solicitando información del metadataset')
                return None
            except Exception as e:
                raise e

            for report in response_data:
                report_code = report['id']
                report_id = report['annotations'][0]['text']
                report_published = report['annotations'][3]['text']

                reports['code'].append(report_code)
                reports['id'].append(report_id)
                reports['published'].append(report_published)
        return pd.DataFrame(data=reports)

    def init_data(self):
        self.reports = self.get(True)

    def download_report(self, report_id):
        url = f'{self.configuracion["direccion_API_SDMX"]}/sdmx_172/client/static/referenceMetadata/template' \
              f'/GenericMetadataTemplate.html?' \
              f'nodeId={self.configuracion["nodeId"]}&metadataSetId={self.id}' \
              f'&reportId={report_id}&lang=es&BaseUrlMDA=' \
              f'{self.configuracion["metadata_api"]}'
        options = webdriver.FirefoxOptions()
        options.add_argument("--headless")
        driver = webdriver.Firefox(options=options)
        driver.get(url)
        driver.implicitly_wait(10)
        driver.find_element(By.ID, 'download-report-button').click()
        driver.implicitly_wait(1)

        with subprocess.Popen(f"MOVE /Y C:\\Users\\index\\Downloads\\" + report_id+".html C:\\IndexaEduardo\\Main_sdmx\\main_sdmx\\sistema_informacion\\metadatos_html",
                              shell=True):
            driver.close()
        # with subprocess.Popen(f"mv $HOME/Downloads/{report_id}.html {self.configuracion['directorio_metadatos_html']}",
        #                       shell=True):
        #     driver.close()

    def get_report(self):
        actividad_consulta = self.id[self.id.find('_') + 1:]
        actividad = actividad_consulta[:actividad_consulta.rfind('_')]
        json_name = 'REPORT_' + actividad_consulta + '.json'
        with open(f'{self.configuracion["directorio_reportes_metadatos"]}/{actividad}/{json_name}', 'r',
                  encoding='utf-8') as json_file, open(self.configuracion['plantilla_reportes'], 'r',
                                                       encoding='utf-8') as template_file:
            reporte = json.load(json_file)
            template = BeautifulSoup(template_file, 'html.parser')
            json_file.close()
            template_file.close()
        return self.__get_report(actividad_consulta,reporte, template)

    def __get_report(self, actividad_consulta, reporte, template):
        reporte = reporte['data']['metadataSets'][0]['reports'][0]['attributeSet']['reportedAttributes']
        # ID del reporte
        template.find(attrs={'id': 'introtable'}).next.next.next.string = f'REPORT_{actividad_consulta}'
        # Referencia al dataflow
        template.find(attrs={'id': 'span-DATAFLOW'}).string = f'DF_{actividad_consulta}'
        # Recorremos todos los puntos R...
        rindex = 1
        i = 0
        j = 0
        while True:
            try:
                aux = reporte[i]['attributeSet']['reportedAttributes']
                while True:
                    try:
                        template.find(attrs={'id': f'R{rindex}-r2'}).string = aux[j]['texts']['es']
                        rindex += 1
                        j += 1
                    except IndexError:
                        j = 0
                        i += 1
                        break
            except IndexError:
                break

        # Escribimos en fichero
        with open(f'{self.configuracion["directorio_metadatos_html"]}/REPORT_{actividad_consulta}.html', 'w',
                  encoding='utf-8') as reporte_file:
            reporte_file.write(str(template))
            reporte_file.close()

    def extract_info_html(self):
        report_id = self.id[self.id.find('_') + 1:]
        with open(f'{self.configuracion["directorio_sistema_informacion"]}descripciones.yaml', 'r',
                  encoding='utf-8') as file, \
                open(f'{self.configuracion["directorio_metadatos_html"]}/REPORT_{report_id}.html', 'r',
                     encoding='utf-8') as report:
            info = yaml.safe_load(file)
            html = BeautifulSoup(report, 'html.parser')
            file.close()
            report.close()
        if report_id not in info:
            info[report_id] = {}
            descripcion = html.find(attrs={"id": "R12-r2"}).text
            info[report_id]['descripcion'] = descripcion
            info[report_id]['tags'] = []
            with open(f'{self.configuracion["directorio_sistema_informacion"]}descripciones.yaml', 'w',
                      encoding='utf-8') as file:
                yaml.dump(info, file)
                file.close()

    def download_all_reports(self):
        self.reports.apply(lambda x: self.download_report(x.code), axis=1)

    def put(self, path):
        with open(path, 'rb') as file:
            body = {'file': ('test.json', file, 'application/json', {})}
            data, content_type = requests.models.RequestEncodingMixin._encode_files(body, {})
            upload_headers = copy.deepcopy(self.session.headers)
            upload_headers['Content-Type'] = content_type
            try:
                response = self.session.post(
                    f'{self.configuracion["url_base"]}api/RM/checkFileJsonMetadataset/{self.id}', data=data,
                    headers=upload_headers)
                response_body = response.json()
                response.raise_for_status()
            except Exception as e:
                raise e
            self.logger.info('Reporte subido correctamente a la API, realizando importacion')
            try:
                response = self.session.post(
                    f'{self.configuracion["url_base"]}api/RM/importFileJsonMetadataset/{self.id}',
                    json=response_body)
                response.raise_for_status()
            except Exception as e:
                raise e

    def publish(self, report):
        self.logger.info('Publicando reporte %s', report)
        body = {'newState': (None, 'PUBLISHED', None, ())}
        data, content_type = requests.models.RequestEncodingMixin._encode_files(body, {})
        upload_headers = copy.deepcopy(self.session.headers)
        upload_headers['Content-Type'] = content_type
        try:
            response = self.session.post(
                f'{self.configuracion["url_base"]}api/RM/updateStateMetReport/{self.id}/{report}',
                headers=upload_headers, data=data)
            response.raise_for_status()
        except Exception as e:
            raise e

    def publish_all(self):
        self.logger.info('Publicando todos los reportes del metadaset con id %s', self.id)
        self.reports.apply(lambda x: self.publish(x.code), axis=1)

    def delete(self):
        self.logger.info('Borrando el metadaset con id %s', self.id)
        self.delete_all_reports()
        try:
            response = self.session.delete(f'{self.configuracion["url_base"]}api/RM/deleteGenericMetadataset/{self.id}')
            response.raise_for_status()
        except Exception as e:
            raise e

    def delete_all_reports(self):
        self.logger.info('Borrando reportes del metadataset')
        self.reports.apply(lambda x: self.delete_report(x.id), axis=1)

    def delete_report(self, report):
        try:
            response = self.session.delete(f'{self.configuracion["url_base"]}api/RM/deleteReport/{report}')
            response.raise_for_status()
        except Exception as e:
            raise e
