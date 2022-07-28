import yaml

from src.mdmpyclient.mdm import MDM

if __name__ == '__main__':
    configuracion = open('configuracion/configuracion.yaml', 'r', encoding='utf-8')
    configuracion = yaml.safe_load(configuracion)

    controller = MDM(configuracion)
    controller.logout()
