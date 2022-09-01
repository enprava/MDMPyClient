import yaml
from IECA-extractor.src.ieca.actividad import Actividad

with open('configuracion/configuracion.yaml') as configuracion:
    configuracion = yaml.safe_load(configuracion)
    extractor_path = configuracion['extractor_path']
with open(f"{extractor_path}configuracion/global.yaml", 'r', encoding='utf-8') as extractor_configuracion_global, \
        open(f"{extractor_path}configuracion/ejecucion.yaml", 'r', encoding='utf-8') as extractor_configuracion_ejecucion, \
        open(f"{extractor_path}configuracion/actividades.yaml", 'r', encoding='utf-8') as extractor_configuracion_actividades, \
        open(f"{extractor_path}configuracion/plantilla_actividad.yaml", 'r',
             encoding='utf-8') as extractor_plantilla_configuracion_actividad, \
        open(f"{extractor_path}sistema_informacion/mapas/conceptos_codelist.yaml", 'r',
             encoding='utf-8') as extractor_mapa_conceptos_codelist:
        # open(f"{extractor_path}sistema_informacion/traducciones.yaml", 'r',
        #      encoding='utf-8') as traducciones:

    extractor_configuracion_global = yaml.safe_load(extractor_configuracion_global)
    extractor_configuracion_ejecucion = yaml.safe_load(extractor_configuracion_ejecucion)
    extractor_configuracion_actividades = yaml.safe_load(extractor_configuracion_actividades)
    extractor_configuracion_plantilla_actividad = yaml.safe_load(extractor_plantilla_configuracion_actividad)
    extractor_mapa_conceptos_codelist = yaml.safe_load(extractor_mapa_conceptos_codelist)
for nombre_actividad in extractor_configuracion_ejecucion['actividades']:
        actividad = Actividad(extractor_configuracion_global, extractor_configuracion_actividades[nombre_actividad],
                              extractor_configuracion_plantilla_actividad, nombre_actividad)
        actividad.generar_consultas()
        actividad.ejecutar()