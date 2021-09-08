import configparser

class Configuration(object):
    """
    Clase que permite cargar dinámicamente los archivos de configuración del proceso.
    """
    
    def __init__(self, file_config="Config.ini", section_names=["DEFAULT"]):
        r"""Inicializa los parametros de entrada
        :param file_config: Ruta absoluta del archivo de configuración.
        :param section_names: (optional) Lista de secciones a cargar.
        :return: :class: object
        """
        parser = configparser.ConfigParser()
        parser.optionxform = str  
        found = parser.read(file_config)

        if not found:
            raise ValueError('Archivo de configuracion no encontrado.')
        
        self.parser = parser

        # Cargando configuracion
        for name in section_names:
            self.__dict__.update(parser.items(name))