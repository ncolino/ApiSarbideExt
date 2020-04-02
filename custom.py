import settings
import pymysql
from datetime import datetime

import logging


#Ejemplos de uso:
#logging.basicConfig(level=logging.DEBUG)
#logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
#logging.debug('This is a debug message')
#logging.info('This is an info message')
#logging.warning('This is a warning message')
#logging.error('This is an error message')
#logging.critical('This is a critical message')

#try:
#  c = a / b
#except Exception as e:
#  logging.error("Exception occurred", exc_info=True)


class Funciones:

    _DEBUG = logging.DEBUG
    _INFO = logging.INFO
    _WARNING = logging.WARNING
    _ERROR = logging.ERROR
    _CRITICAL = logging.CRITICAL

    def FiltrarCaracteres(self, _cadena):
        _cadena = _cadena.replace("\r\n"," ")
        _cadena = _cadena.replace("\r"," ")
        _cadena = _cadena.replace("\n"," ")
        _cadena = _cadena.replace("\"","'")
        _cadena = _cadena.replace("=","-")
        _cadena = _cadena.replace("Í","I")    
        if not _cadena.strip():
            _cadena = ""
        return _cadena

    def EscribeLog(self, _nombre_fichero_log, _mensaje, _log_level):
        ahora = datetime.now()
        _filename = "./logs/" + _nombre_fichero_log + "_" + ahora.strftime("%Y%m%d") + ".log"
        logging.basicConfig(level=settings.LOG_LEVEL, filename=_filename, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')    
        if _log_level == logging.DEBUG:
            logging.debug('%s', _mensaje) 
        elif _log_level == logging.INFO:
            logging.info('%s', _mensaje) 
        elif _log_level == logging.WARNING:
            logging.warning('%s', _mensaje) 
        elif _log_level == logging.ERROR:
            logging.error('%s', _mensaje)
        elif _log_level == logging.CRITICAL:
            logging.critical('%s', _mensaje)  


class Database:
    def __init__(self):
        self.funciones = Funciones()  
        host = settings.MYSQL_DATABASE_HOST
        user = settings.MYSQL_DATABASE_USER
        password = settings.MYSQL_DATABASE_PASSWORD
        db = settings.MYSQL_DATABASE_DB
        self.con = pymysql.connect(host=host, user=user, password=password, db=db, cursorclass=pymysql.cursors.
                                   DictCursor)
        self.cur = self.con.cursor()

    def ConsultaAssetListByProjectId(self,_id):        
        self.cur.execute("SELECT ASSET_ID FROM PROJECT_ASSET WHERE PROJECT_ID=%s", _id)
        #results = self.cur.fetchall()  --> usamos mejor el cursor
        result = []
        for row in self.cur:
            result.append(row['ASSET_ID'])
        return result   

    def ConsultaExtMediaByAssetId(self,_id):        
        self.cur.execute("""SELECT distinct EXT_MEDIA.ID_EXT_MEDIA,EXT_MEDIA.ID,EXT_MEDIA.ASSET_ID,DATE_FORMAT(EXT_MEDIA.ASSET_VALUE, '%%Y-%%m-%%d %%H:%%i:%%s') AS ASSET_VALUE,
            EXT_MEDIA.FORMATO1_ID,EXT_MEDIA.FORMATO2_ID,EXT_MEDIA.CONTENIDO1_ID,EXT_MEDIA.CONTENIDO2_ID,EXT_MEDIA.TITLE_10,EXT_MEDIA.DURATION,EXT_MEDIA.SCRIPT_DESCRIPTION,
            EXT_MEDIA.REMARKS,EXT_MEDIA.NESCALETA,EXT_MEDIA.NNOTICIA,EXT_MEDIA.ANYO_ESC,EXT_MEDIA.NOMBRE_PROG,EXT_MEDIA.OFF,EXT_MEDIA.SYNOPSIS_SAR,EXT_MEDIA.PRODUCT_CODE,EXT_MEDIA.CHAPTER,
            EXT_MEDIA.MEDIA_UTI_CRE,EXT_MEDIA.MEDIA_UTI_MOD,DATE_FORMAT(EXT_MEDIA.MEDIA_CRE, '%%Y-%%m-%%d %%H:%%i:%%s') AS MEDIA_CRE,DATE_FORMAT(EXT_MEDIA.MEDIA_MOD, '%%Y-%%m-%%d %%H:%%i:%%s') AS MEDIA_MOD,
            EXT_MEDIA.ID_EXT_MEDIA_TYPE,EXT_MEDIA_TYPE.NAME AS EXT_MEDIA_TYPE 
            FROM EXT_MEDIA LEFT JOIN EXT_MEDIA_TYPE ON EXT_MEDIA_TYPE.ID_EXT_MEDIA_TYPE = EXT_MEDIA.ID_EXT_MEDIA_TYPE WHERE ASSET_ID=%s""", _id)
        #results = self.cur.fetchall()  --> usamos mejor el cursor        
        result = []
        for row in self.cur:            
            row['TITLE_10'] = self.funciones.FiltrarCaracteres(row['TITLE_10']) 
            row['SCRIPT_DESCRIPTION'] = self.funciones.FiltrarCaracteres(row['SCRIPT_DESCRIPTION']) 
            row['REMARKS'] = self.funciones.FiltrarCaracteres(row['REMARKS']) 
            row['OFF'] = self.funciones.FiltrarCaracteres(row['OFF']) 
            row['SYNOPSIS_SAR'] = self.funciones.FiltrarCaracteres(row['SYNOPSIS_SAR'])             
            #result.append(row)
            result = row
            break
        return result    

    def ConsultaExtMediaFullByAssetId(self,_id):        
        self.cur.execute("""SELECT distinct EXT_MEDIA.ID_EXT_MEDIA,EXT_MEDIA.ID,EXT_MEDIA.ASSET_ID,DATE_FORMAT(EXT_MEDIA.ASSET_VALUE, '%%Y-%%m-%%d %%H:%%i:%%s') AS ASSET_VALUE,EXT_MEDIA.FORMATO1_ID,EXT_MEDIA.FORMATO2_ID,EXT_MEDIA.CONTENIDO1_ID,EXT_MEDIA.CONTENIDO2_ID
			,EXT_MEDIA.TITLE_10,EXT_MEDIA.DURATION,EXT_MEDIA.SCRIPT_DESCRIPTION,EXT_MEDIA.REMARKS,EXT_MEDIA.NESCALETA,EXT_MEDIA.NNOTICIA,EXT_MEDIA.ANYO_ESC,EXT_MEDIA.NOMBRE_PROG,EXT_MEDIA.OFF,EXT_MEDIA.SYNOPSIS_SAR,EXT_MEDIA.PRODUCT_CODE
			,EXT_MEDIA.CHAPTER,EXT_MEDIA.MEDIA_UTI_CRE,EXT_MEDIA.MEDIA_UTI_MOD,DATE_FORMAT(EXT_MEDIA.MEDIA_CRE, '%%Y-%%m-%%d %%H:%%i:%%s') AS MEDIA_CRE,DATE_FORMAT(EXT_MEDIA.MEDIA_MOD, '%%Y-%%m-%%d %%H:%%i:%%s') AS MEDIA_MOD,EXT_MEDIA.ID_EXT_MEDIA_TYPE
			,EXT_MEDIA_TYPE.NAME AS EXT_MEDIA_TYPE, MP43_QUERY.MP43_MEDIA_VODURL, MP43_QUERY.MP43_PATH, MP43_QUERY.MP43_FILENAMEEXT, WEBM_QUERY.WEBM_MEDIA_VODURL, WEBM_QUERY.WEBM_PATH, WEBM_QUERY.WEBM_FILENAMEEXT, HLS_QUERY.HLS_RENDITION_URL
            ,THUMBNAIL_QUERY.THUMBNAIL_MEDIA_VOD_URL, THUMBNAIL_QUERY.THUMBNAIL_PATH, THUMBNAIL_QUERY.THUMBNAIL_NAME, SPRITE_QUERY.SPRITE_MEDIA_VOD_URL, SPRITE_QUERY.SPRITE_PATH, SPRITE_QUERY.SPRITE_NAME
			FROM EXT_MEDIA
			LEFT JOIN EXT_MEDIA_TYPE ON EXT_MEDIA_TYPE.ID_EXT_MEDIA_TYPE = EXT_MEDIA.ID_EXT_MEDIA_TYPE   
            LEFT JOIN (
                   SELECT EXT_QUALITY.ID_EXT_MEDIA,EXT_QUALITY.FRAME_WIDTH,EXT_QUALITY.FRAME_HEIGHT,PUB_CONFIG.VOD_URL,
					PUB_CONFIG.MEDIA_VOD_URL AS WEBM_MEDIA_VODURL,EXT_SUPPORT.PATH AS WEBM_PATH,EXT_QUALITY.NAME AS WEBM_FILENAMEEXT,EXT_QUALITY.ENCODING_RATE
					FROM EXT_MEDIA,EXT_QUALITY, EXT_SUPPORT, EXT_SERVER, PUB_CONFIG
					WHERE EXT_MEDIA.ID_EXT_MEDIA = EXT_QUALITY.ID_EXT_MEDIA
                    AND PUB_CONFIG.ID_EXT_SERVER = EXT_SERVER.ID_EXT_SERVER
					AND EXT_SERVER.ID_EXT_SERVER = EXT_SUPPORT.ID_EXT_SERVER
					AND EXT_SUPPORT.ID_EXT_SUPPORT = EXT_QUALITY.ID_EXT_SUPPORT
                    AND EXT_MEDIA.ASSET_ID = %s
                    AND EXT_QUALITY.ID_EXT_QUALITY_TYPE = 2
			) WEBM_QUERY
		    ON WEBM_QUERY.ID_EXT_MEDIA = EXT_MEDIA.ID_EXT_MEDIA
            LEFT JOIN (
                   SELECT EXT_QUALITY.ID_EXT_MEDIA,EXT_QUALITY.FRAME_WIDTH,EXT_QUALITY.FRAME_HEIGHT,PUB_CONFIG.VOD_URL,
					PUB_CONFIG.MEDIA_VOD_URL AS MP43_MEDIA_VODURL,EXT_SUPPORT.PATH AS MP43_PATH,EXT_QUALITY.NAME AS MP43_FILENAMEEXT,EXT_QUALITY.ENCODING_RATE
					FROM EXT_MEDIA, EXT_QUALITY, EXT_SUPPORT, EXT_SERVER, PUB_CONFIG
					WHERE EXT_MEDIA.ID_EXT_MEDIA = EXT_QUALITY.ID_EXT_MEDIA
                    AND PUB_CONFIG.ID_EXT_SERVER = EXT_SERVER.ID_EXT_SERVER
					AND EXT_SERVER.ID_EXT_SERVER = EXT_SUPPORT.ID_EXT_SERVER
					AND EXT_SUPPORT.ID_EXT_SUPPORT = EXT_QUALITY.ID_EXT_SUPPORT
                    AND EXT_MEDIA.ASSET_ID = %s
					AND EXT_QUALITY.ID_EXT_QUALITY_TYPE = 1
			) MP43_QUERY
		    ON MP43_QUERY.ID_EXT_MEDIA = EXT_MEDIA.ID_EXT_MEDIA
            LEFT JOIN (
				 SELECT CMS_RENDITION.RENDITION_URL AS HLS_RENDITION_URL, EXT_QUALITY_CMS_RENDITION.ID_EXT_MEDIA                      
				 from  EXT_MEDIA, CMS_RENDITION, EXT_QUALITY_CMS_RENDITION              
				 where EXT_MEDIA.ID_EXT_MEDIA = EXT_QUALITY_CMS_RENDITION.ID_EXT_MEDIA
                 AND CMS_RENDITION.ID_CMS_RENDITION=EXT_QUALITY_CMS_RENDITION.ID_CMS_RENDITION
                 AND EXT_MEDIA.ASSET_ID = %s
				 AND CMS_RENDITION.ID_PUBLICATION_TYPE = 2
            ) HLS_QUERY
            ON HLS_QUERY.ID_EXT_MEDIA = EXT_MEDIA.ID_EXT_MEDIA
            LEFT JOIN (
				 SELECT EXT_IMAGE.ID_EXT_MEDIA, PUB_CONFIG.MEDIA_VOD_URL AS THUMBNAIL_MEDIA_VOD_URL,EXT_SUPPORT.PATH AS THUMBNAIL_PATH,EXT_IMAGE.NAME AS THUMBNAIL_NAME,EXT_IMAGE.TIME_CODE,EXT_IMAGE.WIDTH AS IWIDTH,EXT_IMAGE.HEIGHT AS IHEIGHT,EXT_IMAGE.IMAGE_SIZE,EXT_IMAGE_TYPE.NAME AS ITYPE
				,EXT_SERVER.ID_EXT_SERVER,EXT_IMAGE.ID_EXT_IMAGE
				FROM EXT_MEDIA, EXT_SUPPORT,EXT_SERVER,EXT_IMAGE,EXT_IMAGE_TYPE, PUB_CONFIG
				WHERE EXT_MEDIA.ID_EXT_MEDIA = EXT_IMAGE.ID_EXT_MEDIA
                AND PUB_CONFIG.ID_EXT_SERVER = EXT_SERVER.ID_EXT_SERVER
				AND EXT_IMAGE.ID_EXT_SUPPORT= EXT_SUPPORT.ID_EXT_SUPPORT
				AND EXT_SUPPORT.ID_EXT_SERVER=EXT_SERVER.ID_EXT_SERVER
                AND EXT_MEDIA.ASSET_ID = %s
				AND EXT_IMAGE.ID_EXT_IMAGE_TYPE = 3
            )THUMBNAIL_QUERY
            ON THUMBNAIL_QUERY.ID_EXT_MEDIA = EXT_MEDIA.ID_EXT_MEDIA
            LEFT JOIN (
				 SELECT EXT_IMAGE.ID_EXT_MEDIA, PUB_CONFIG.MEDIA_VOD_URL AS SPRITE_MEDIA_VOD_URL,EXT_SUPPORT.PATH AS SPRITE_PATH,EXT_IMAGE.NAME AS SPRITE_NAME,EXT_IMAGE.TIME_CODE,EXT_IMAGE.WIDTH AS IWIDTH,EXT_IMAGE.HEIGHT AS IHEIGHT,EXT_IMAGE.IMAGE_SIZE,EXT_IMAGE_TYPE.NAME AS ITYPE
				,EXT_SERVER.ID_EXT_SERVER,EXT_IMAGE.ID_EXT_IMAGE
				FROM EXT_MEDIA,EXT_SUPPORT,EXT_SERVER,EXT_IMAGE,EXT_IMAGE_TYPE, PUB_CONFIG
				WHERE EXT_MEDIA.ID_EXT_MEDIA = EXT_IMAGE.ID_EXT_MEDIA
                AND PUB_CONFIG.ID_EXT_SERVER = EXT_SERVER.ID_EXT_SERVER
				AND EXT_IMAGE.ID_EXT_SUPPORT= EXT_SUPPORT.ID_EXT_SUPPORT
				AND EXT_SUPPORT.ID_EXT_SERVER=EXT_SERVER.ID_EXT_SERVER
                AND EXT_MEDIA.ASSET_ID = %s
				AND EXT_IMAGE.ID_EXT_IMAGE_TYPE = 1
            )SPRITE_QUERY
            ON SPRITE_QUERY.ID_EXT_MEDIA = EXT_MEDIA.ID_EXT_MEDIA
            WHERE EXT_MEDIA.ASSET_ID = %s""", (_id, _id, _id, _id, _id, _id))
        #results = self.cur.fetchall()  --> usamos mejor el cursor        
        result = []
        for row in self.cur:     
            _mp43_media_vodurl = row['MP43_MEDIA_VODURL']
            _mp43_path = row['MP43_PATH']
            _mp43_filenameext = row['MP43_FILENAMEEXT']  
            _webm_media_vodurl = row['WEBM_MEDIA_VODURL']
            _webm_path = row['WEBM_PATH']
            _webm_filenameext = row['WEBM_FILENAMEEXT']      
            _thumbnail_media_vod_url = row['THUMBNAIL_MEDIA_VOD_URL']
            _thumbnail_path = row['THUMBNAIL_PATH']
            _thumbnail_name = row['THUMBNAIL_NAME']                       
            _sprite_media_vod_url = row['SPRITE_MEDIA_VOD_URL']
            _sprite_path = row['SPRITE_PATH']
            _sprite_name = row['SPRITE_NAME']                       
            
            mp4_pmd_url = ""
            webm_url = ""
            thumbnail_url = ""
            sprite_url = ""

            if _mp43_media_vodurl:
                if _mp43_path:
                    if _mp43_filenameext:
                       mp4_pmd_url = _mp43_media_vodurl + _mp43_path + _mp43_filenameext
                       mp4_pmd_url = mp4_pmd_url.replace("http://","https://")

            if _webm_media_vodurl:
                if _webm_path:
                    if _webm_filenameext:
                       webm_url = _webm_media_vodurl + _webm_path + _webm_filenameext
                       webm_url = webm_url.replace("http://","https://")
        
            if _thumbnail_media_vod_url:
                if _thumbnail_path:
                    if _thumbnail_name:                      
                       thumbnail_url = _thumbnail_media_vod_url + _thumbnail_path + _thumbnail_name
                       thumbnail_url = thumbnail_url.replace("http://","https://")
                       
            if _sprite_media_vod_url:
                if _sprite_path:
                    if _sprite_name:                      
                       sprite_url = _sprite_media_vod_url + _sprite_path + _sprite_name
                       sprite_url = sprite_url.replace("http://","https://")
           
            row['MP4PMD_URL'] = mp4_pmd_url
            row['WEBMPMD_URL'] = webm_url            
            row['THUMBNAIL_URL'] = thumbnail_url
            row['SPRITE_URL'] = sprite_url

            row['TITLE_10'] = self.funciones.FiltrarCaracteres(row['TITLE_10']) 
            row['SCRIPT_DESCRIPTION'] = self.funciones.FiltrarCaracteres(row['SCRIPT_DESCRIPTION']) 
            row['REMARKS'] = self.funciones.FiltrarCaracteres(row['REMARKS']) 
            row['OFF'] = self.funciones.FiltrarCaracteres(row['OFF']) 
            row['SYNOPSIS_SAR'] = self.funciones.FiltrarCaracteres(row['SYNOPSIS_SAR'])             
            #result.append(row)
            result = row
            break
        return result    

class Metodos:    
    def __init__(self):
        self.funciones = Funciones()            

    def funcion_GetAssetListByProjectId(self,_id):        
        self.funciones.EscribeLog(settings.LOG_FILENAME, '{}{}'.format('INICIO funcion_GetAssetListByProjectId - _id: ', _id), self.funciones._INFO)  
        db = Database()  
        asss = db.ConsultaAssetListByProjectId(_id)        
        self.funciones.EscribeLog(settings.LOG_FILENAME, '{}{}'.format('funcion_GetAssetListByProjectId - assets: ', str(asss)), self.funciones._INFO)    
        return asss
        
    def funcion_GetExtMediaByAssetId(self,_id):
        self.funciones.EscribeLog(settings.LOG_FILENAME, '{}{}'.format('INICIO funcion_GetExtMediaByAssetId - _id: ', _id), self.funciones._INFO)
        db = Database()  
        asss = db.ConsultaExtMediaByAssetId(_id)        
        self.funciones.EscribeLog(settings.LOG_FILENAME, '{}{}'.format('funcion_GetExtMediaByAssetId - ext_media: ', str(asss)), self.funciones._INFO)  
        return asss

    def funcion_GetExtMediaFullByAssetId(self,_id):
        self.funciones.EscribeLog(settings.LOG_FILENAME, '{}{}'.format('INICIO funcion_GetExtMediaFullByAssetId - _id: ', _id), self.funciones._INFO)  
        db = Database()  
        asss = db.ConsultaExtMediaFullByAssetId(_id)        
        self.funciones.EscribeLog(settings.LOG_FILENAME, '{}{}'.format('funcion_GetExtMediaFullByAssetId - ext_media: ', str(asss)), self.funciones._INFO)  
        return asss
        