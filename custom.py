import settings
import pymysql
from datetime import datetime


import mysql.connector
from mysql.connector import errorcode


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
        #host = settings.MYSQL_DATABASE_HOST
        #user = settings.MYSQL_DATABASE_USER
        #password = settings.MYSQL_DATABASE_PASSWORD
        #db = settings.MYSQL_DATABASE_DB
        #self.con = pymysql.connect(host=host, user=user, password=password, db=db, cursorclass=pymysql.cursors.DictCursor)
        #self.cur = self.con.cursor()

    def Connect_mysql(self):
        host = settings.MYSQL_DATABASE_HOST
        user = settings.MYSQL_DATABASE_USER
        password = settings.MYSQL_DATABASE_PASSWORD
        db = settings.MYSQL_DATABASE_DB

        try:
            self.con = mysql.connector.connect(user=user, password=password, host=host, database=db, autocommit =True, raise_on_warnings=True,pool_name = "mypool", pool_size = 20)
            self.cur = self.con.cursor()
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
            

        #self.con = pymysql.connect(host=host, user=user, password=password, db=db, cursorclass=pymysql.cursors.DictCursor)
        #self.cur = self.con.cursor()

    def Connect_pymysql(self):
        host = settings.MYSQL_DATABASE_HOST
        user = settings.MYSQL_DATABASE_USER
        password = settings.MYSQL_DATABASE_PASSWORD
        db = settings.MYSQL_DATABASE_DB
        self.con = pymysql.connect(host=host, user=user, password=password, db=db, cursorclass=pymysql.cursors.DictCursor)
        self.cur = self.con.cursor()
    

    def Disconnect(self):        
        self.con.close()


    def ConsultaAssetListByProjectId(self, _id, _email):        
        self.cur.execute("""SELECT PROJECT_ASSET.ASSET_ID AS ASSET_ID 
        FROM PROJECT_ASSET
        JOIN EXT_MEDIA ON EXT_MEDIA.ASSET_ID=PROJECT_ASSET.ASSET_ID 
        JOIN NODE_GROUPUSER_MEDIA ON NODE_GROUPUSER_MEDIA.PROJECT_ID = PROJECT_ASSET.PROJECT_ID
        JOIN GROUPUSER_USERS ON NODE_GROUPUSER_MEDIA.GROUP_USER_ID = GROUPUSER_USERS.GROUP_USER_ID
        JOIN USERS ON USERS.USER_ID = GROUPUSER_USERS.USER_ID
        WHERE EXT_MEDIA.ID IS NOT NULL 
        AND PROJECT_ASSET.PROJECT_ID = %s
        AND USERS.E_CORREO = %s
        ORDER BY PROJECT_ASSET.FECHA_CRE DESC""", (_id, _email))
        #results = self.cur.fetchall()  --> usamos mejor el cursor
        result = []
        for row in self.cur:
            result.append(row['ASSET_ID'])
        self.cur.close()
        return result   

    def ConsultaExtMediaByAssetId(self,_id,_email):        
        self.cur.execute("""SELECT distinct EXT_MEDIA.ID_EXT_MEDIA,EXT_MEDIA.ID,EXT_MEDIA.ASSET_ID,DATE_FORMAT(EXT_MEDIA.ASSET_VALUE, '%%Y-%%m-%%d %%H:%%i:%%s') AS ASSET_VALUE,
            EXT_MEDIA.FORMATO1_ID AS F1_ID,EXT_MEDIA.FORMATO2_ID AS F2_ID,CONTENIDO1.CONTENIDO1_EUS AS C1,CONTENIDO2.CONTENIDO2_EUS AS C2,FORMATO1.FORMATO1_EUS AS F1,FORMATO2.FORMATO2_EUS AS F2,
            EXT_MEDIA.CONTENIDO1_ID AS C1_ID,EXT_MEDIA.CONTENIDO2_ID AS C2_ID,EXT_MEDIA.TITLE_10,EXT_MEDIA.DURATION,EXT_MEDIA.SCRIPT_DESCRIPTION,
            EXT_MEDIA.REMARKS,EXT_MEDIA.NESCALETA,EXT_MEDIA.NNOTICIA,EXT_MEDIA.ANYO_ESC,EXT_MEDIA.NOMBRE_PROG,EXT_MEDIA.OFF,EXT_MEDIA.SYNOPSIS_SAR,EXT_MEDIA.PRODUCT_CODE,EXT_MEDIA.CHAPTER,
            EXT_MEDIA.MEDIA_UTI_CRE,EXT_MEDIA.MEDIA_UTI_MOD,DATE_FORMAT(EXT_MEDIA.MEDIA_CRE, '%%Y-%%m-%%d %%H:%%i:%%s') AS MEDIA_CRE,DATE_FORMAT(EXT_MEDIA.MEDIA_MOD, '%%Y-%%m-%%d %%H:%%i:%%s') AS MEDIA_MOD,
            EXT_MEDIA.ID_EXT_MEDIA_TYPE,EXT_MEDIA_TYPE.NAME AS EXT_MEDIA_TYPE 
            FROM EXT_MEDIA 
            JOIN PROJECT_ASSET ON EXT_MEDIA.ASSET_ID = PROJECT_ASSET.ASSET_ID
            JOIN NODE_GROUPUSER_MEDIA ON NODE_GROUPUSER_MEDIA.PROJECT_ID = PROJECT_ASSET.PROJECT_ID
            JOIN GROUPUSER_USERS ON GROUPUSER_USERS.GROUP_USER_ID = NODE_GROUPUSER_MEDIA.GROUP_USER_ID
            JOIN USERS ON USERS.USER_ID = GROUPUSER_USERS.USER_ID
            LEFT JOIN FORMATO2 ON (EXT_MEDIA.FORMATO1_ID=FORMATO2.FORMATO1_ID AND EXT_MEDIA.FORMATO2_ID=FORMATO2.FORMATO2_ID) 
            LEFT JOIN FORMATO1 ON EXT_MEDIA.FORMATO1_ID=FORMATO1.FORMATO1_ID 
            LEFT JOIN CONTENIDO2 ON (EXT_MEDIA.CONTENIDO1_ID=CONTENIDO2.CONTENIDO1_ID AND EXT_MEDIA.CONTENIDO2_ID=CONTENIDO2.CONTENIDO2_ID) 
            LEFT JOIN CONTENIDO1 ON EXT_MEDIA.CONTENIDO1_ID=CONTENIDO1.CONTENIDO1_ID 
            LEFT JOIN EXT_MEDIA_TYPE ON EXT_MEDIA_TYPE.ID_EXT_MEDIA_TYPE = EXT_MEDIA.ID_EXT_MEDIA_TYPE 
            WHERE EXT_MEDIA.ASSET_ID = %s
            AND USERS.E_CORREO = %s""", (_id, _email))
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
        self.cur.close()
        return result    


    def ConsultaExtMediaFullByAssetId_mysql(self,_id,_email,_extended):
        self.cur.execute("""SELECT distinct EXT_MEDIA.ID_EXT_MEDIA,EXT_MEDIA.ASSET_ID,EXT_MEDIA.ASSET_VALUE
			,EXT_MEDIA.FORMATO1_ID AS F1_ID,EXT_MEDIA.FORMATO2_ID AS F2_ID,CONTENIDO1.CONTENIDO1_EUS AS C1,CONTENIDO2.CONTENIDO2_EUS AS C2,FORMATO1.FORMATO1_EUS AS F1,FORMATO2.FORMATO2_EUS AS F2
            ,EXT_MEDIA.CONTENIDO1_ID AS C1_ID,EXT_MEDIA.CONTENIDO2_ID AS C2_ID
            ,EXT_MEDIA.TITLE_10,EXT_MEDIA.DURATION,EXT_MEDIA.SCRIPT_DESCRIPTION,EXT_MEDIA.REMARKS,EXT_MEDIA.NESCALETA,EXT_MEDIA.NNOTICIA,EXT_MEDIA.ANYO_ESC,EXT_MEDIA.NOMBRE_PROG,EXT_MEDIA.OFF,EXT_MEDIA.SYNOPSIS_SAR,EXT_MEDIA.PRODUCT_CODE			
            ,EXT_MEDIA.CHAPTER,EXT_MEDIA.MEDIA_UTI_CRE,EXT_MEDIA.MEDIA_UTI_MOD,EXT_MEDIA.MEDIA_CRE,MEDIA_MOD,EXT_MEDIA.ID_EXT_MEDIA_TYPE			            
			FROM EXT_MEDIA 			
            LEFT JOIN FORMATO2 ON (EXT_MEDIA.FORMATO1_ID=FORMATO2.FORMATO1_ID AND EXT_MEDIA.FORMATO2_ID=FORMATO2.FORMATO2_ID) 
            LEFT JOIN FORMATO1 ON EXT_MEDIA.FORMATO1_ID=FORMATO1.FORMATO1_ID 
            LEFT JOIN CONTENIDO2 ON (EXT_MEDIA.CONTENIDO1_ID=CONTENIDO2.CONTENIDO1_ID AND EXT_MEDIA.CONTENIDO2_ID=CONTENIDO2.CONTENIDO2_ID) 
            LEFT JOIN CONTENIDO1 ON EXT_MEDIA.CONTENIDO1_ID=CONTENIDO1.CONTENIDO1_ID
            WHERE EXT_MEDIA.ID is not null            
			AND EXT_MEDIA.ASSET_ID = %s
            AND EXT_MEDIA.ASSET_ID IN (
				SELECT ASSET_ID FROM PROJECT_ASSET
				INNER JOIN NODE_GROUPUSER_MEDIA ON NODE_GROUPUSER_MEDIA.PROJECT_ID = PROJECT_ASSET.PROJECT_ID
				INNER JOIN GROUPUSER_USERS ON GROUPUSER_USERS.GROUP_USER_ID = NODE_GROUPUSER_MEDIA.GROUP_USER_ID
				INNER JOIN USERS ON USERS.USER_ID = GROUPUSER_USERS.USER_ID			
				WHERE USERS.E_CORREO = %s         
            ) LIMIT 1""", (_id, _email))
            
        result = []        

        for (_id_ext_media, _asset_id, _asset_value, _f1_id, _f2_id, _c1, _c2, _f1, _f2, _c1_id, _c2_id, _title_10, _duration, _script_description, _remarks, _nescaleta, _nnoticia, _anyo_esc, _nombre_prog, _off, _synopsis_sar, _product_code
        , _chapter, _media_uti_cre, _media_uti_mod, _media_cre, _media_mod, _id_ext_media_type) in self.cur:

            ext_media_type = "VIDEO"
            if _id_ext_media_type == 2:
                ext_media_type = "AUDIO"
            if _id_ext_media_type == 3:
                ext_media_type = "IMAGEN"

            _title_10 = self.funciones.FiltrarCaracteres(_title_10) 
            _script_description = self.funciones.FiltrarCaracteres(_script_description) 
            _remarks = self.funciones.FiltrarCaracteres(_remarks) 
            _off = self.funciones.FiltrarCaracteres(_off) 
            _synopsis_sar = self.funciones.FiltrarCaracteres(_synopsis_sar)    

            if _extended:
                new_row = {
                    "ID_EXT_MEDIA": _id_ext_media
                    , "ASSET_ID": _asset_id
                    , "ASSET_VALUE": _asset_value.strftime('%Y-%m-%d-%H:%M:%S')
                    , "F1_ID": _f1_id
                    , "F2_ID": _f2_id
                    , "C1_ID": _c1_id
                    , "C2_ID": _c2_id
                    , "F1": _f1
                    , "F2": _f2
                    , "C1": _c1
                    , "C2": _c2
                    , "TITLE_10": _title_10
                    , "DURATION": _duration
                    , "SCRIPT_DESCRIPTION": _script_description
                    , "REMARKS": _remarks
                    , "NESCALETA": _nescaleta
                    , "NNOTICIA": _nnoticia
                    , "ANYO_ESC": _anyo_esc
                    , "NOMBRE_PROG": _nombre_prog
                    , "OFF": _off
                    , "SYNOPSIS_SAR": _synopsis_sar
                    , "PRODUCT_CODE": _product_code
                    , "CHAPTER": _chapter
                    , "MEDIA_CRE": _media_cre.strftime('%Y-%m-%d-%H:%M:%S')
                    , "MEDIA_MOD": _media_mod.strftime('%Y-%m-%d-%H:%M:%S')
                    , "ID_EXT_MEDIA_TYPE": _id_ext_media_type
                    , "EXT_MEDIA_TYPE": ext_media_type 
                    , "HLS_RENDITION_URL": ""
                    , "MP4PMD_URL": ""
                    , "WEBMPMD_URL": ""
                    , "THUMBNAIL_URL": ""
                    , "STILL_URL": ""
                    , "SPRITE_URL": ""             
                }                
                result = new_row
            else:
                new_row = {
                    "ID_EXT_MEDIA": _id_ext_media
                    , "ASSET_ID": _asset_id
                    , "ASSET_VALUE": _asset_value              
                    , "TITLE_10": _title_10
                    , "DURATION": _duration                  
                    , "EXT_MEDIA_TYPE": ext_media_type
                    , "HLS_RENDITION_URL": ""
                    , "MP4PMD_URL": ""
                    , "WEBMPMD_URL": ""
                    , "THUMBNAIL_URL": ""
                    , "STILL_URL": ""
                    , "SPRITE_URL": ""             
                }                
                result = new_row
            break                            

        if len(result) == 0:
            result = []    
        else:                       
            #rellenar url MP4 VOD URL
            self.cur.execute("""SELECT PUB_CONFIG.MEDIA_VOD_URL AS MP43_MEDIA_VODURL,EXT_SUPPORT.PATH AS MP43_PATH,EXT_QUALITY.NAME AS MP43_FILENAMEEXT
					FROM EXT_MEDIA, EXT_QUALITY, EXT_SUPPORT, EXT_SERVER, PUB_CONFIG
					WHERE EXT_MEDIA.ID_EXT_MEDIA = EXT_QUALITY.ID_EXT_MEDIA
                    AND PUB_CONFIG.ID_EXT_SERVER = EXT_SERVER.ID_EXT_SERVER
					AND EXT_SERVER.ID_EXT_SERVER = EXT_SUPPORT.ID_EXT_SERVER
					AND EXT_SUPPORT.ID_EXT_SUPPORT = EXT_QUALITY.ID_EXT_SUPPORT
                    AND EXT_MEDIA.ASSET_ID = %s
					AND EXT_QUALITY.ID_EXT_QUALITY_TYPE = %s		                    
                    LIMIT 1""", (_id, 1))                            

            for (_mp43_media_vodurl, _mp43_path, _mp43_filenameext) in self.cur:                
                mp4_pmd_url = ""               

                if _mp43_media_vodurl:
                    if _mp43_path:
                        if _mp43_filenameext:
                            mp4_pmd_url = _mp43_media_vodurl + _mp43_path + _mp43_filenameext
                            mp4_pmd_url = mp4_pmd_url.replace("http://","https://")
                
                result['MP4PMD_URL'] = mp4_pmd_url     
                break            
            

            #rellenar url WEBM URL            
            self.cur.execute("""SELECT PUB_CONFIG.MEDIA_VOD_URL AS WEBM_MEDIA_VODURL,EXT_SUPPORT.PATH AS WEBM_PATH,EXT_QUALITY.NAME AS WEBM_FILENAMEEXT
					FROM EXT_MEDIA, EXT_QUALITY, EXT_SUPPORT, EXT_SERVER, PUB_CONFIG
					WHERE EXT_MEDIA.ID_EXT_MEDIA = EXT_QUALITY.ID_EXT_MEDIA
                    AND PUB_CONFIG.ID_EXT_SERVER = EXT_SERVER.ID_EXT_SERVER
					AND EXT_SERVER.ID_EXT_SERVER = EXT_SUPPORT.ID_EXT_SERVER
					AND EXT_SUPPORT.ID_EXT_SUPPORT = EXT_QUALITY.ID_EXT_SUPPORT
                    AND EXT_MEDIA.ASSET_ID = %s
					AND EXT_QUALITY.ID_EXT_QUALITY_TYPE = %s		                    
                    LIMIT 1""", (_id, 2)) 
                    
            for (_webm_media_vodurl, _webm_path, _webm_filenameext) in self.cur:
                webm_url = ""               

                if _webm_media_vodurl:
                    if _webm_path:
                        if _webm_filenameext:
                            webm_url = _webm_media_vodurl + _webm_path + _webm_filenameext
                            webm_url = webm_url.replace("http://","https://")
                
                result['WEBMPMD_URL'] = webm_url    
                break   
                   
            #rellenar url WEBM URL           
            self.cur.execute("""SELECT CMS_RENDITION.RENDITION_URL AS HLS_RENDITION_URL                      
				    from  EXT_MEDIA, CMS_RENDITION, EXT_QUALITY_CMS_RENDITION              
				    where EXT_MEDIA.ID_EXT_MEDIA = EXT_QUALITY_CMS_RENDITION.ID_EXT_MEDIA
                    AND CMS_RENDITION.ID_CMS_RENDITION=EXT_QUALITY_CMS_RENDITION.ID_CMS_RENDITION
                    AND EXT_MEDIA.ASSET_ID = %s
				    AND CMS_RENDITION.ID_PUBLICATION_TYPE = %s                   		                    
                    LIMIT 1""", (_id, 2))  
                    
            for (_hls_rendition_url) in self.cur:                              
                result['HLS_RENDITION_URL'] = _hls_rendition_url      
                break
                        
            #rellenar url THUMBNAIL URL            
            self.cur.execute("""SELECT PUB_CONFIG.MEDIA_VOD_URL AS THUMBNAIL_MEDIA_VOD_URL,EXT_SUPPORT.PATH AS THUMBNAIL_PATH,EXT_IMAGE.NAME AS THUMBNAIL_NAME
				FROM EXT_MEDIA, EXT_SUPPORT,EXT_SERVER, EXT_IMAGE, PUB_CONFIG
				WHERE EXT_MEDIA.ID_EXT_MEDIA = EXT_IMAGE.ID_EXT_MEDIA
                AND PUB_CONFIG.ID_EXT_SERVER = EXT_SERVER.ID_EXT_SERVER
				AND EXT_IMAGE.ID_EXT_SUPPORT= EXT_SUPPORT.ID_EXT_SUPPORT
				AND EXT_SUPPORT.ID_EXT_SERVER=EXT_SERVER.ID_EXT_SERVER
                AND EXT_MEDIA.ASSET_ID = %s
				AND EXT_IMAGE.ID_EXT_IMAGE_TYPE = %s		                    
                LIMIT 1""", (_id, 3)) 
                    
            for (_thumbnail_media_vod_url, _thumbnail_path, _thumbnail_name) in self.cur:
                thumbnail_url = ""               

                if _thumbnail_media_vod_url:
                    if _thumbnail_path:
                        if _thumbnail_name:                      
                            thumbnail_url = _thumbnail_media_vod_url + _thumbnail_path + _thumbnail_name
                            thumbnail_url = thumbnail_url.replace("http://","https://")
               
                result['THUMBNAIL_URL'] = thumbnail_url     
                break                       

            #rellenar url STILL URL            
            self.cur.execute("""SELECT PUB_CONFIG.MEDIA_VOD_URL AS STILL_MEDIA_VOD_URL,EXT_SUPPORT.PATH AS STILL_PATH,EXT_IMAGE.NAME AS STILL_NAME
				FROM EXT_MEDIA, EXT_SUPPORT,EXT_SERVER, EXT_IMAGE, PUB_CONFIG
				WHERE EXT_MEDIA.ID_EXT_MEDIA = EXT_IMAGE.ID_EXT_MEDIA
                AND PUB_CONFIG.ID_EXT_SERVER = EXT_SERVER.ID_EXT_SERVER
				AND EXT_IMAGE.ID_EXT_SUPPORT= EXT_SUPPORT.ID_EXT_SUPPORT
				AND EXT_SUPPORT.ID_EXT_SERVER=EXT_SERVER.ID_EXT_SERVER
                AND EXT_MEDIA.ASSET_ID = %s
				AND EXT_IMAGE.ID_EXT_IMAGE_TYPE = %s		                    
                LIMIT 1""", (_id, 2))  
                    
            for (_still_media_vod_url, _still_path, _still_name) in self.cur:
                still_url = ""               

                if _still_media_vod_url:
                    if _still_path:
                        if _still_name:                      
                            still_url = _still_media_vod_url + _still_path + _still_name
                            still_url = still_url.replace("http://","https://")
               
                result['STILL_URL'] = still_url 
                break                            

            #rellenar url SPRITE URL            
            self.cur.execute("""SELECT PUB_CONFIG.MEDIA_VOD_URL AS SPRITE_MEDIA_VOD_URL,EXT_SUPPORT.PATH AS SPRITE_PATH,EXT_IMAGE.NAME AS SPRITE_NAME
				FROM EXT_MEDIA, EXT_SUPPORT,EXT_SERVER, EXT_IMAGE, PUB_CONFIG
				WHERE EXT_MEDIA.ID_EXT_MEDIA = EXT_IMAGE.ID_EXT_MEDIA
                AND PUB_CONFIG.ID_EXT_SERVER = EXT_SERVER.ID_EXT_SERVER
				AND EXT_IMAGE.ID_EXT_SUPPORT= EXT_SUPPORT.ID_EXT_SUPPORT
				AND EXT_SUPPORT.ID_EXT_SERVER=EXT_SERVER.ID_EXT_SERVER
                AND EXT_MEDIA.ASSET_ID = %s
				AND EXT_IMAGE.ID_EXT_IMAGE_TYPE = %s		                    
                LIMIT 1""", (_id, 1)) 
                    
            for (_sprite_media_vod_url, _sprite_path, _sprite_name) in self.cur:
                sprite_url = ""               

                if _sprite_media_vod_url:
                    if _sprite_path:
                        if _sprite_name:                      
                            sprite_url = _sprite_media_vod_url + _sprite_path + _sprite_name
                            sprite_url = sprite_url.replace("http://","https://")           
                
                result['SPRITE_URL'] = sprite_url     
                break                      
               
        return result 

    def ConsultaExtMediaFullByAssetId_pymysql(self,_id,_email,_extended):     
        self.cur.execute("""SELECT distinct EXT_MEDIA.ID_EXT_MEDIA,EXT_MEDIA.ASSET_ID,DATE_FORMAT(EXT_MEDIA.ASSET_VALUE, '%%Y-%%m-%%d %%H:%%i:%%s') AS ASSET_VALUE
			,EXT_MEDIA.FORMATO1_ID AS F1_ID,EXT_MEDIA.FORMATO2_ID AS F2_ID,CONTENIDO1.CONTENIDO1_EUS AS C1,CONTENIDO2.CONTENIDO2_EUS AS C2,FORMATO1.FORMATO1_EUS AS F1,FORMATO2.FORMATO2_EUS AS F2
            ,EXT_MEDIA.CONTENIDO1_ID AS C1_ID,EXT_MEDIA.CONTENIDO2_ID AS C2_ID
            ,EXT_MEDIA.TITLE_10,EXT_MEDIA.DURATION,EXT_MEDIA.SCRIPT_DESCRIPTION,EXT_MEDIA.REMARKS,EXT_MEDIA.NESCALETA,EXT_MEDIA.NNOTICIA,EXT_MEDIA.ANYO_ESC,EXT_MEDIA.NOMBRE_PROG,EXT_MEDIA.OFF,EXT_MEDIA.SYNOPSIS_SAR,EXT_MEDIA.PRODUCT_CODE			
            ,EXT_MEDIA.CHAPTER,EXT_MEDIA.MEDIA_UTI_CRE,EXT_MEDIA.MEDIA_UTI_MOD,DATE_FORMAT(EXT_MEDIA.MEDIA_CRE, '%%Y-%%m-%%d %%H:%%i:%%s') AS MEDIA_CRE,DATE_FORMAT(EXT_MEDIA.MEDIA_MOD, '%%Y-%%m-%%d %%H:%%i:%%s') AS MEDIA_MOD,EXT_MEDIA.ID_EXT_MEDIA_TYPE			            
			FROM EXT_MEDIA 			
            LEFT JOIN FORMATO2 ON (EXT_MEDIA.FORMATO1_ID=FORMATO2.FORMATO1_ID AND EXT_MEDIA.FORMATO2_ID=FORMATO2.FORMATO2_ID) 
            LEFT JOIN FORMATO1 ON EXT_MEDIA.FORMATO1_ID=FORMATO1.FORMATO1_ID 
            LEFT JOIN CONTENIDO2 ON (EXT_MEDIA.CONTENIDO1_ID=CONTENIDO2.CONTENIDO1_ID AND EXT_MEDIA.CONTENIDO2_ID=CONTENIDO2.CONTENIDO2_ID) 
            LEFT JOIN CONTENIDO1 ON EXT_MEDIA.CONTENIDO1_ID=CONTENIDO1.CONTENIDO1_ID
            WHERE EXT_MEDIA.ID is not null            
			AND EXT_MEDIA.ASSET_ID = %s
            AND EXT_MEDIA.ASSET_ID IN (
				SELECT ASSET_ID FROM PROJECT_ASSET
				INNER JOIN NODE_GROUPUSER_MEDIA ON NODE_GROUPUSER_MEDIA.PROJECT_ID = PROJECT_ASSET.PROJECT_ID
				INNER JOIN GROUPUSER_USERS ON GROUPUSER_USERS.GROUP_USER_ID = NODE_GROUPUSER_MEDIA.GROUP_USER_ID
				INNER JOIN USERS ON USERS.USER_ID = GROUPUSER_USERS.USER_ID			
				WHERE USERS.E_CORREO = %s         
            ) LIMIT 1""", (_id, _email))
                   
        result = []
        for row in self.cur:                                         
            ext_media_type = "VIDEO"
            if row['ID_EXT_MEDIA_TYPE'] == 2:
                ext_media_type = "AUDIO"
            if row['ID_EXT_MEDIA_TYPE'] == 3:
                ext_media_type = "IMAGEN"
                       

            row['TITLE_10'] = self.funciones.FiltrarCaracteres(row['TITLE_10']) 
            row['SCRIPT_DESCRIPTION'] = self.funciones.FiltrarCaracteres(row['SCRIPT_DESCRIPTION']) 
            row['REMARKS'] = self.funciones.FiltrarCaracteres(row['REMARKS']) 
            row['OFF'] = self.funciones.FiltrarCaracteres(row['OFF']) 
            row['SYNOPSIS_SAR'] = self.funciones.FiltrarCaracteres(row['SYNOPSIS_SAR'])        

            if _extended:
                new_row = {
                    "ID_EXT_MEDIA": row['ID_EXT_MEDIA']
                    , "ASSET_ID": row['ASSET_ID']
                    , "ASSET_VALUE": row['ASSET_VALUE']
                    , "F1_ID": row['F1_ID']
                    , "F2_ID": row['F2_ID']
                    , "C1_ID": row['C1_ID']
                    , "C2_ID": row['C2_ID']
                    , "F1": row['F1']
                    , "F2": row['F2']
                    , "C1": row['C1']
                    , "C2": row['C2']
                    , "TITLE_10": row['TITLE_10']
                    , "DURATION": row['DURATION']
                    , "SCRIPT_DESCRIPTION": row['SCRIPT_DESCRIPTION']
                    , "REMARKS": row['REMARKS']
                    , "NESCALETA": row['NESCALETA']
                    , "NNOTICIA": row['NNOTICIA']
                    , "ANYO_ESC": row['ANYO_ESC']
                    , "NOMBRE_PROG": row['NOMBRE_PROG']
                    , "OFF": row['OFF']
                    , "SYNOPSIS_SAR": row['SYNOPSIS_SAR']
                    , "PRODUCT_CODE": row['PRODUCT_CODE']
                    , "CHAPTER": row['CHAPTER']
                    , "MEDIA_CRE": row['MEDIA_CRE']
                    , "MEDIA_MOD": row['MEDIA_MOD']
                    , "ID_EXT_MEDIA_TYPE": row['ID_EXT_MEDIA_TYPE']
                    , "EXT_MEDIA_TYPE": ext_media_type 
                    , "HLS_RENDITION_URL": ""
                    , "MP4PMD_URL": ""
                    , "WEBMPMD_URL": ""
                    , "THUMBNAIL_URL": ""
                    , "STILL_URL": ""
                    , "SPRITE_URL": ""            
                }
                #result.append(row)
                result = new_row
            else:
                new_row = {
                    "ID_EXT_MEDIA": row['ID_EXT_MEDIA']
                    , "ASSET_ID": row['ASSET_ID']
                    , "ASSET_VALUE": row['ASSET_VALUE']                   
                    , "TITLE_10": row['TITLE_10']
                    , "DURATION": row['DURATION']                    
                    , "EXT_MEDIA_TYPE": ext_media_type
                    , "HLS_RENDITION_URL": ""
                    , "MP4PMD_URL": ""
                    , "WEBMPMD_URL": ""
                    , "THUMBNAIL_URL": ""
                    , "STILL_URL": ""
                    , "SPRITE_URL": ""           
                }
                #result.append(row)
                result = new_row
            break

        if len(result) == 0:
            result = []    
        else:                       
            #rellenar url MP4 VOD URL
            self.cur.execute("""SELECT PUB_CONFIG.MEDIA_VOD_URL AS MP43_MEDIA_VODURL,EXT_SUPPORT.PATH AS MP43_PATH,EXT_QUALITY.NAME AS MP43_FILENAMEEXT
					FROM EXT_MEDIA, EXT_QUALITY, EXT_SUPPORT, EXT_SERVER, PUB_CONFIG
					WHERE EXT_MEDIA.ID_EXT_MEDIA = EXT_QUALITY.ID_EXT_MEDIA
                    AND PUB_CONFIG.ID_EXT_SERVER = EXT_SERVER.ID_EXT_SERVER
					AND EXT_SERVER.ID_EXT_SERVER = EXT_SUPPORT.ID_EXT_SERVER
					AND EXT_SUPPORT.ID_EXT_SUPPORT = EXT_QUALITY.ID_EXT_SUPPORT
                    AND EXT_MEDIA.ASSET_ID = %s
					AND EXT_QUALITY.ID_EXT_QUALITY_TYPE = %s		                    
                    LIMIT 1""", (_id, 1))                            

            for row in self.cur:  
                _mp43_media_vodurl = row['MP43_MEDIA_VODURL']
                _mp43_path = row['MP43_PATH']
                _mp43_filenameext = row['MP43_FILENAMEEXT']                                     
                
                mp4_pmd_url = ""                         

                if _mp43_media_vodurl:
                    if _mp43_path:
                        if _mp43_filenameext:
                            mp4_pmd_url = _mp43_media_vodurl + _mp43_path + _mp43_filenameext
                            mp4_pmd_url = mp4_pmd_url.replace("http://","https://")
                
                result['MP4PMD_URL'] = mp4_pmd_url     
                break            
            

            #rellenar url WEBM URL            
            self.cur.execute("""SELECT PUB_CONFIG.MEDIA_VOD_URL AS WEBM_MEDIA_VODURL,EXT_SUPPORT.PATH AS WEBM_PATH,EXT_QUALITY.NAME AS WEBM_FILENAMEEXT
					FROM EXT_MEDIA, EXT_QUALITY, EXT_SUPPORT, EXT_SERVER, PUB_CONFIG
					WHERE EXT_MEDIA.ID_EXT_MEDIA = EXT_QUALITY.ID_EXT_MEDIA
                    AND PUB_CONFIG.ID_EXT_SERVER = EXT_SERVER.ID_EXT_SERVER
					AND EXT_SERVER.ID_EXT_SERVER = EXT_SUPPORT.ID_EXT_SERVER
					AND EXT_SUPPORT.ID_EXT_SUPPORT = EXT_QUALITY.ID_EXT_SUPPORT
                    AND EXT_MEDIA.ASSET_ID = %s
					AND EXT_QUALITY.ID_EXT_QUALITY_TYPE = %s		                    
                    LIMIT 1""", (_id, 2)) 
                    
            for row in self.cur:               
                _webm_media_vodurl = row['WEBM_MEDIA_VODURL']
                _webm_path = row['WEBM_PATH']
                _webm_filenameext = row['WEBM_FILENAMEEXT']      
                              
                webm_url = ""               

                if _webm_media_vodurl:
                    if _webm_path:
                        if _webm_filenameext:
                            webm_url = _webm_media_vodurl + _webm_path + _webm_filenameext
                            webm_url = webm_url.replace("http://","https://")
                
                result['WEBMPMD_URL'] = webm_url    
                break   
                   
            #rellenar url WEBM URL           
            self.cur.execute("""SELECT CMS_RENDITION.RENDITION_URL AS HLS_RENDITION_URL                      
				    from  EXT_MEDIA, CMS_RENDITION, EXT_QUALITY_CMS_RENDITION              
				    where EXT_MEDIA.ID_EXT_MEDIA = EXT_QUALITY_CMS_RENDITION.ID_EXT_MEDIA
                    AND CMS_RENDITION.ID_CMS_RENDITION=EXT_QUALITY_CMS_RENDITION.ID_CMS_RENDITION
                    AND EXT_MEDIA.ASSET_ID = %s
				    AND CMS_RENDITION.ID_PUBLICATION_TYPE = %s                   		                    
                    LIMIT 1""", (_id, 2))  
                    
            for row in self.cur:                                                            
                result['HLS_RENDITION_URL'] = row['HLS_RENDITION_URL']      
                break
                        
            #rellenar url THUMBNAIL URL            
            self.cur.execute("""SELECT PUB_CONFIG.MEDIA_VOD_URL AS THUMBNAIL_MEDIA_VOD_URL,EXT_SUPPORT.PATH AS THUMBNAIL_PATH,EXT_IMAGE.NAME AS THUMBNAIL_NAME
				FROM EXT_MEDIA, EXT_SUPPORT,EXT_SERVER, EXT_IMAGE, PUB_CONFIG
				WHERE EXT_MEDIA.ID_EXT_MEDIA = EXT_IMAGE.ID_EXT_MEDIA
                AND PUB_CONFIG.ID_EXT_SERVER = EXT_SERVER.ID_EXT_SERVER
				AND EXT_IMAGE.ID_EXT_SUPPORT= EXT_SUPPORT.ID_EXT_SUPPORT
				AND EXT_SUPPORT.ID_EXT_SERVER=EXT_SERVER.ID_EXT_SERVER
                AND EXT_MEDIA.ASSET_ID = %s
				AND EXT_IMAGE.ID_EXT_IMAGE_TYPE = %s		                    
                LIMIT 1""", (_id, 3)) 
                    
            for row in self.cur:                
                _thumbnail_media_vod_url = row['THUMBNAIL_MEDIA_VOD_URL']
                _thumbnail_path = row['THUMBNAIL_PATH']
                _thumbnail_name = row['THUMBNAIL_NAME']     

                thumbnail_url = ""               

                if _thumbnail_media_vod_url:
                    if _thumbnail_path:
                        if _thumbnail_name:                      
                            thumbnail_url = _thumbnail_media_vod_url + _thumbnail_path + _thumbnail_name
                            thumbnail_url = thumbnail_url.replace("http://","https://")
               
                result['THUMBNAIL_URL'] = thumbnail_url     
                break                       

            #rellenar url STILL URL            
            self.cur.execute("""SELECT PUB_CONFIG.MEDIA_VOD_URL AS STILL_MEDIA_VOD_URL,EXT_SUPPORT.PATH AS STILL_PATH,EXT_IMAGE.NAME AS STILL_NAME
				FROM EXT_MEDIA, EXT_SUPPORT,EXT_SERVER, EXT_IMAGE, PUB_CONFIG
				WHERE EXT_MEDIA.ID_EXT_MEDIA = EXT_IMAGE.ID_EXT_MEDIA
                AND PUB_CONFIG.ID_EXT_SERVER = EXT_SERVER.ID_EXT_SERVER
				AND EXT_IMAGE.ID_EXT_SUPPORT= EXT_SUPPORT.ID_EXT_SUPPORT
				AND EXT_SUPPORT.ID_EXT_SERVER=EXT_SERVER.ID_EXT_SERVER
                AND EXT_MEDIA.ASSET_ID = %s
				AND EXT_IMAGE.ID_EXT_IMAGE_TYPE = %s		                    
                LIMIT 1""", (_id, 2))  
                    
            for row in self.cur:       
                _still_media_vod_url = row['STILL_MEDIA_VOD_URL']
                _still_path = row['STILL_PATH']
                _still_name = row['STILL_NAME']                       
               
                still_url = ""               

                if _still_media_vod_url:
                    if _still_path:
                        if _still_name:                      
                            still_url = _still_media_vod_url + _still_path + _still_name
                            still_url = still_url.replace("http://","https://")
               
                result['STILL_URL'] = still_url 
                break                            

            #rellenar url SPRITE URL            
            self.cur.execute("""SELECT PUB_CONFIG.MEDIA_VOD_URL AS SPRITE_MEDIA_VOD_URL,EXT_SUPPORT.PATH AS SPRITE_PATH,EXT_IMAGE.NAME AS SPRITE_NAME
				FROM EXT_MEDIA, EXT_SUPPORT,EXT_SERVER, EXT_IMAGE, PUB_CONFIG
				WHERE EXT_MEDIA.ID_EXT_MEDIA = EXT_IMAGE.ID_EXT_MEDIA
                AND PUB_CONFIG.ID_EXT_SERVER = EXT_SERVER.ID_EXT_SERVER
				AND EXT_IMAGE.ID_EXT_SUPPORT= EXT_SUPPORT.ID_EXT_SUPPORT
				AND EXT_SUPPORT.ID_EXT_SERVER=EXT_SERVER.ID_EXT_SERVER
                AND EXT_MEDIA.ASSET_ID = %s
				AND EXT_IMAGE.ID_EXT_IMAGE_TYPE = %s		                    
                LIMIT 1""", (_id, 1)) 
                    
            for row in self.cur:                   
                _sprite_media_vod_url = row['SPRITE_MEDIA_VOD_URL']
                _sprite_path = row['SPRITE_PATH']
                _sprite_name = row['SPRITE_NAME']                       
                               
                sprite_url = ""               

                if _sprite_media_vod_url:
                    if _sprite_path:
                        if _sprite_name:                      
                            sprite_url = _sprite_media_vod_url + _sprite_path + _sprite_name
                            sprite_url = sprite_url.replace("http://","https://")           
                
                result['SPRITE_URL'] = sprite_url     
                break                      
                       
        return result    

   

    def ConsultaExtMediaFullByAssetId_mysql_old(self,_id,_email,_extended):     
        #self.cur.execute("""SELECT distinct EXT_MEDIA.ID_EXT_MEDIA,EXT_MEDIA.ID,EXT_MEDIA.ASSET_ID,DATE_FORMAT(EXT_MEDIA.ASSET_VALUE, '%%Y-%%m-%%d %%H:%%i:%%s') AS ASSET_VALUE
        #,EXT_MEDIA.CHAPTER,EXT_MEDIA.MEDIA_UTI_CRE,EXT_MEDIA.MEDIA_UTI_MOD,DATE_FORMAT(EXT_MEDIA.MEDIA_CRE, '%%Y-%%m-%%d %%H:%%i:%%s') AS MEDIA_CRE,DATE_FORMAT(EXT_MEDIA.MEDIA_MOD, '%%Y-%%m-%%d %%H:%%i:%%s') AS MEDIA_MOD,EXT_MEDIA.ID_EXT_MEDIA_TYPE			
        self.cur.execute("""SELECT distinct EXT_MEDIA.ID_EXT_MEDIA,EXT_MEDIA.ID,EXT_MEDIA.ASSET_ID,EXT_MEDIA.ASSET_VALUE
			,EXT_MEDIA.FORMATO1_ID AS F1_ID,EXT_MEDIA.FORMATO2_ID AS F2_ID,CONTENIDO1.CONTENIDO1_EUS AS C1,CONTENIDO2.CONTENIDO2_EUS AS C2,FORMATO1.FORMATO1_EUS AS F1,FORMATO2.FORMATO2_EUS AS F2
            ,EXT_MEDIA.CONTENIDO1_ID AS C1_ID,EXT_MEDIA.CONTENIDO2_ID AS C2_ID
            ,EXT_MEDIA.TITLE_10,EXT_MEDIA.DURATION,EXT_MEDIA.SCRIPT_DESCRIPTION,EXT_MEDIA.REMARKS,EXT_MEDIA.NESCALETA,EXT_MEDIA.NNOTICIA,EXT_MEDIA.ANYO_ESC,EXT_MEDIA.NOMBRE_PROG,EXT_MEDIA.OFF,EXT_MEDIA.SYNOPSIS_SAR,EXT_MEDIA.PRODUCT_CODE			
            ,EXT_MEDIA.CHAPTER,EXT_MEDIA.MEDIA_UTI_CRE,EXT_MEDIA.MEDIA_UTI_MOD,EXT_MEDIA.MEDIA_CRE,MEDIA_MOD,EXT_MEDIA.ID_EXT_MEDIA_TYPE			
            , MP43_QUERY.MP43_MEDIA_VODURL, MP43_QUERY.MP43_PATH, MP43_QUERY.MP43_FILENAMEEXT, WEBM_QUERY.WEBM_MEDIA_VODURL, WEBM_QUERY.WEBM_PATH, WEBM_QUERY.WEBM_FILENAMEEXT, HLS_QUERY.HLS_RENDITION_URL
            ,THUMBNAIL_QUERY.THUMBNAIL_MEDIA_VOD_URL, THUMBNAIL_QUERY.THUMBNAIL_PATH, THUMBNAIL_QUERY.THUMBNAIL_NAME,STILL_QUERY.STILL_MEDIA_VOD_URL, STILL_QUERY.STILL_PATH, STILL_QUERY.STILL_NAME, SPRITE_QUERY.SPRITE_MEDIA_VOD_URL, SPRITE_QUERY.SPRITE_PATH, SPRITE_QUERY.SPRITE_NAME
			FROM EXT_MEDIA 			
            LEFT JOIN FORMATO2 ON (EXT_MEDIA.FORMATO1_ID=FORMATO2.FORMATO1_ID AND EXT_MEDIA.FORMATO2_ID=FORMATO2.FORMATO2_ID) 
            LEFT JOIN FORMATO1 ON EXT_MEDIA.FORMATO1_ID=FORMATO1.FORMATO1_ID 
            LEFT JOIN CONTENIDO2 ON (EXT_MEDIA.CONTENIDO1_ID=CONTENIDO2.CONTENIDO1_ID AND EXT_MEDIA.CONTENIDO2_ID=CONTENIDO2.CONTENIDO2_ID) 
            LEFT JOIN CONTENIDO1 ON EXT_MEDIA.CONTENIDO1_ID=CONTENIDO1.CONTENIDO1_ID 					
            LEFT JOIN (
                   SELECT EXT_QUALITY.ID_EXT_MEDIA,EXT_QUALITY.FRAME_WIDTH,EXT_QUALITY.FRAME_HEIGHT,PUB_CONFIG.VOD_URL,
					PUB_CONFIG.MEDIA_VOD_URL AS WEBM_MEDIA_VODURL,EXT_SUPPORT.PATH AS WEBM_PATH,EXT_QUALITY.NAME AS WEBM_FILENAMEEXT,EXT_QUALITY.ENCODING_RATE
					FROM EXT_MEDIA,EXT_QUALITY, EXT_SUPPORT, EXT_SERVER, PUB_CONFIG
					WHERE EXT_MEDIA.ID_EXT_MEDIA = EXT_QUALITY.ID_EXT_MEDIA
                    AND PUB_CONFIG.ID_EXT_SERVER = EXT_SERVER.ID_EXT_SERVER
					AND EXT_SERVER.ID_EXT_SERVER = EXT_SUPPORT.ID_EXT_SERVER
					AND EXT_SUPPORT.ID_EXT_SUPPORT = EXT_QUALITY.ID_EXT_SUPPORT
                    AND EXT_MEDIA.ASSET_ID = %s
                    AND EXT_QUALITY.ID_EXT_QUALITY_TYPE = %s
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
					AND EXT_QUALITY.ID_EXT_QUALITY_TYPE = %s
			) MP43_QUERY
		    ON MP43_QUERY.ID_EXT_MEDIA = EXT_MEDIA.ID_EXT_MEDIA
            LEFT JOIN (
				 SELECT CMS_RENDITION.RENDITION_URL AS HLS_RENDITION_URL, EXT_QUALITY_CMS_RENDITION.ID_EXT_MEDIA                      
				 from  EXT_MEDIA, CMS_RENDITION, EXT_QUALITY_CMS_RENDITION              
				 where EXT_MEDIA.ID_EXT_MEDIA = EXT_QUALITY_CMS_RENDITION.ID_EXT_MEDIA
                 AND CMS_RENDITION.ID_CMS_RENDITION=EXT_QUALITY_CMS_RENDITION.ID_CMS_RENDITION
                 AND EXT_MEDIA.ASSET_ID = %s
				 AND CMS_RENDITION.ID_PUBLICATION_TYPE = %s
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
				AND EXT_IMAGE.ID_EXT_IMAGE_TYPE = %s
            )THUMBNAIL_QUERY
            ON THUMBNAIL_QUERY.ID_EXT_MEDIA = EXT_MEDIA.ID_EXT_MEDIA
            LEFT JOIN (
				 SELECT EXT_IMAGE.ID_EXT_MEDIA, PUB_CONFIG.MEDIA_VOD_URL AS STILL_MEDIA_VOD_URL,EXT_SUPPORT.PATH AS STILL_PATH,EXT_IMAGE.NAME AS STILL_NAME,EXT_IMAGE.TIME_CODE,EXT_IMAGE.WIDTH AS IWIDTH,EXT_IMAGE.HEIGHT AS IHEIGHT,EXT_IMAGE.IMAGE_SIZE,EXT_IMAGE_TYPE.NAME AS ITYPE
				,EXT_SERVER.ID_EXT_SERVER,EXT_IMAGE.ID_EXT_IMAGE
				FROM EXT_MEDIA, EXT_SUPPORT,EXT_SERVER,EXT_IMAGE,EXT_IMAGE_TYPE, PUB_CONFIG
				WHERE EXT_MEDIA.ID_EXT_MEDIA = EXT_IMAGE.ID_EXT_MEDIA
                AND PUB_CONFIG.ID_EXT_SERVER = EXT_SERVER.ID_EXT_SERVER
				AND EXT_IMAGE.ID_EXT_SUPPORT= EXT_SUPPORT.ID_EXT_SUPPORT
				AND EXT_SUPPORT.ID_EXT_SERVER=EXT_SERVER.ID_EXT_SERVER
                AND EXT_MEDIA.ASSET_ID = %s
				AND EXT_IMAGE.ID_EXT_IMAGE_TYPE = %s
            )STILL_QUERY
            ON STILL_QUERY.ID_EXT_MEDIA = EXT_MEDIA.ID_EXT_MEDIA
            LEFT JOIN (
				 SELECT EXT_IMAGE.ID_EXT_MEDIA, PUB_CONFIG.MEDIA_VOD_URL AS SPRITE_MEDIA_VOD_URL,EXT_SUPPORT.PATH AS SPRITE_PATH,EXT_IMAGE.NAME AS SPRITE_NAME,EXT_IMAGE.TIME_CODE,EXT_IMAGE.WIDTH AS IWIDTH,EXT_IMAGE.HEIGHT AS IHEIGHT,EXT_IMAGE.IMAGE_SIZE,EXT_IMAGE_TYPE.NAME AS ITYPE
				,EXT_SERVER.ID_EXT_SERVER,EXT_IMAGE.ID_EXT_IMAGE
				FROM EXT_MEDIA,EXT_SUPPORT,EXT_SERVER,EXT_IMAGE,EXT_IMAGE_TYPE, PUB_CONFIG
				WHERE EXT_MEDIA.ID_EXT_MEDIA = EXT_IMAGE.ID_EXT_MEDIA
                AND PUB_CONFIG.ID_EXT_SERVER = EXT_SERVER.ID_EXT_SERVER
				AND EXT_IMAGE.ID_EXT_SUPPORT= EXT_SUPPORT.ID_EXT_SUPPORT
				AND EXT_SUPPORT.ID_EXT_SERVER=EXT_SERVER.ID_EXT_SERVER
                AND EXT_MEDIA.ASSET_ID = %s
				AND EXT_IMAGE.ID_EXT_IMAGE_TYPE = %s
            )SPRITE_QUERY
            ON SPRITE_QUERY.ID_EXT_MEDIA = EXT_MEDIA.ID_EXT_MEDIA               
            WHERE EXT_MEDIA.ID is not null            
			AND EXT_MEDIA.ASSET_ID = %s
            AND EXT_MEDIA.ASSET_ID IN (
				SELECT ASSET_ID FROM PROJECT_ASSET
				INNER JOIN NODE_GROUPUSER_MEDIA ON NODE_GROUPUSER_MEDIA.PROJECT_ID = PROJECT_ASSET.PROJECT_ID
				INNER JOIN GROUPUSER_USERS ON GROUPUSER_USERS.GROUP_USER_ID = NODE_GROUPUSER_MEDIA.GROUP_USER_ID
				INNER JOIN USERS ON USERS.USER_ID = GROUPUSER_USERS.USER_ID			
				WHERE USERS.E_CORREO = %s         
            ) LIMIT 1""", (_id, 2, _id, 1, _id, 2, _id, 3, _id, 2, _id, 1, _id, _email))

          

        #self.cur.execute("""SELECT distinct EXT_MEDIA.ID_EXT_MEDIA,EXT_MEDIA.ID,EXT_MEDIA.ASSET_ID,DATE_FORMAT(EXT_MEDIA.ASSET_VALUE, '%%Y-%%m-%%d %%H:%%i:%%s') AS ASSET_VALUE,EXT_MEDIA.FORMATO1_ID AS F1_ID,EXT_MEDIA.FORMATO2_ID AS F2_ID,CONTENIDO1.CONTENIDO1_EUS AS C1,CONTENIDO2.CONTENIDO2_EUS AS C2,FORMATO1.FORMATO1_EUS AS F1,FORMATO2.FORMATO2_EUS AS F2,
        #    EXT_MEDIA.CONTENIDO1_ID AS C1_ID,EXT_MEDIA.CONTENIDO2_ID AS C2_ID,EXT_MEDIA.TITLE_10,EXT_MEDIA.DURATION,EXT_MEDIA.SCRIPT_DESCRIPTION,EXT_MEDIA.REMARKS,EXT_MEDIA.NESCALETA,EXT_MEDIA.NNOTICIA,EXT_MEDIA.ANYO_ESC,EXT_MEDIA.NOMBRE_PROG,EXT_MEDIA.OFF,EXT_MEDIA.SYNOPSIS_SAR,EXT_MEDIA.PRODUCT_CODE
		#    ,EXT_MEDIA.CHAPTER,EXT_MEDIA.MEDIA_UTI_CRE,EXT_MEDIA.MEDIA_UTI_MOD,DATE_FORMAT(EXT_MEDIA.MEDIA_CRE, '%%Y-%%m-%%d %%H:%%i:%%s') AS MEDIA_CRE,DATE_FORMAT(EXT_MEDIA.MEDIA_MOD, '%%Y-%%m-%%d %%H:%%i:%%s') AS MEDIA_MOD,EXT_MEDIA.ID_EXT_MEDIA_TYPE
		#	,EXT_MEDIA_TYPE.NAME AS EXT_MEDIA_TYPE, MP43_QUERY.MP43_MEDIA_VODURL, MP43_QUERY.MP43_PATH, MP43_QUERY.MP43_FILENAMEEXT, WEBM_QUERY.WEBM_MEDIA_VODURL, WEBM_QUERY.WEBM_PATH, WEBM_QUERY.WEBM_FILENAMEEXT, HLS_QUERY.HLS_RENDITION_URL
        #    ,THUMBNAIL_QUERY.THUMBNAIL_MEDIA_VOD_URL, THUMBNAIL_QUERY.THUMBNAIL_PATH, THUMBNAIL_QUERY.THUMBNAIL_NAME,STILL_QUERY.STILL_MEDIA_VOD_URL, STILL_QUERY.STILL_PATH, STILL_QUERY.STILL_NAME, SPRITE_QUERY.SPRITE_MEDIA_VOD_URL, SPRITE_QUERY.SPRITE_PATH, SPRITE_QUERY.SPRITE_NAME
		#	FROM EXT_MEDIA 
        #    JOIN PROJECT_ASSET ON EXT_MEDIA.ASSET_ID = PROJECT_ASSET.ASSET_ID
        #    JOIN NODE_GROUPUSER_MEDIA ON NODE_GROUPUSER_MEDIA.PROJECT_ID = PROJECT_ASSET.PROJECT_ID
        #    JOIN GROUPUSER_USERS ON GROUPUSER_USERS.GROUP_USER_ID = NODE_GROUPUSER_MEDIA.GROUP_USER_ID
        #    JOIN USERS ON USERS.USER_ID = GROUPUSER_USERS.USER_ID
        #    LEFT JOIN FORMATO2 ON (EXT_MEDIA.FORMATO1_ID=FORMATO2.FORMATO1_ID AND EXT_MEDIA.FORMATO2_ID=FORMATO2.FORMATO2_ID) 
        #    LEFT JOIN FORMATO1 ON EXT_MEDIA.FORMATO1_ID=FORMATO1.FORMATO1_ID 
        #    LEFT JOIN CONTENIDO2 ON (EXT_MEDIA.CONTENIDO1_ID=CONTENIDO2.CONTENIDO1_ID AND EXT_MEDIA.CONTENIDO2_ID=CONTENIDO2.CONTENIDO2_ID) 
        #    LEFT JOIN CONTENIDO1 ON EXT_MEDIA.CONTENIDO1_ID=CONTENIDO1.CONTENIDO1_ID 
		#	LEFT JOIN EXT_MEDIA_TYPE ON EXT_MEDIA_TYPE.ID_EXT_MEDIA_TYPE = EXT_MEDIA.ID_EXT_MEDIA_TYPE   
        #    LEFT JOIN (
        #           SELECT EXT_QUALITY.ID_EXT_MEDIA,EXT_QUALITY.FRAME_WIDTH,EXT_QUALITY.FRAME_HEIGHT,PUB_CONFIG.VOD_URL,
		#			PUB_CONFIG.MEDIA_VOD_URL AS WEBM_MEDIA_VODURL,EXT_SUPPORT.PATH AS WEBM_PATH,EXT_QUALITY.NAME AS WEBM_FILENAMEEXT,EXT_QUALITY.ENCODING_RATE
		#			FROM EXT_MEDIA,EXT_QUALITY, EXT_SUPPORT, EXT_SERVER, PUB_CONFIG
		#			WHERE EXT_MEDIA.ID_EXT_MEDIA = EXT_QUALITY.ID_EXT_MEDIA
        #            AND PUB_CONFIG.ID_EXT_SERVER = EXT_SERVER.ID_EXT_SERVER
		#			AND EXT_SERVER.ID_EXT_SERVER = EXT_SUPPORT.ID_EXT_SERVER
		#			AND EXT_SUPPORT.ID_EXT_SUPPORT = EXT_QUALITY.ID_EXT_SUPPORT
        #            AND EXT_MEDIA.ASSET_ID = %s
        #            AND EXT_QUALITY.ID_EXT_QUALITY_TYPE = %s
		#	) WEBM_QUERY
		#    ON WEBM_QUERY.ID_EXT_MEDIA = EXT_MEDIA.ID_EXT_MEDIA
        #    LEFT JOIN (
        #           SELECT EXT_QUALITY.ID_EXT_MEDIA,EXT_QUALITY.FRAME_WIDTH,EXT_QUALITY.FRAME_HEIGHT,PUB_CONFIG.VOD_URL,
		#			PUB_CONFIG.MEDIA_VOD_URL AS MP43_MEDIA_VODURL,EXT_SUPPORT.PATH AS MP43_PATH,EXT_QUALITY.NAME AS MP43_FILENAMEEXT,EXT_QUALITY.ENCODING_RATE
		#			FROM EXT_MEDIA, EXT_QUALITY, EXT_SUPPORT, EXT_SERVER, PUB_CONFIG
		#			WHERE EXT_MEDIA.ID_EXT_MEDIA = EXT_QUALITY.ID_EXT_MEDIA
        #            AND PUB_CONFIG.ID_EXT_SERVER = EXT_SERVER.ID_EXT_SERVER
		#			AND EXT_SERVER.ID_EXT_SERVER = EXT_SUPPORT.ID_EXT_SERVER
		#			AND EXT_SUPPORT.ID_EXT_SUPPORT = EXT_QUALITY.ID_EXT_SUPPORT
        #            AND EXT_MEDIA.ASSET_ID = %s
		#			AND EXT_QUALITY.ID_EXT_QUALITY_TYPE = %s
		#	) MP43_QUERY
		#    ON MP43_QUERY.ID_EXT_MEDIA = EXT_MEDIA.ID_EXT_MEDIA
        #    LEFT JOIN (
		#		 SELECT CMS_RENDITION.RENDITION_URL AS HLS_RENDITION_URL, EXT_QUALITY_CMS_RENDITION.ID_EXT_MEDIA                      
		#		 from  EXT_MEDIA, CMS_RENDITION, EXT_QUALITY_CMS_RENDITION              
		#		 where EXT_MEDIA.ID_EXT_MEDIA = EXT_QUALITY_CMS_RENDITION.ID_EXT_MEDIA
        #         AND CMS_RENDITION.ID_CMS_RENDITION=EXT_QUALITY_CMS_RENDITION.ID_CMS_RENDITION
        #         AND EXT_MEDIA.ASSET_ID = %s
		#		 AND CMS_RENDITION.ID_PUBLICATION_TYPE = %s
        #    ) HLS_QUERY
        #    ON HLS_QUERY.ID_EXT_MEDIA = EXT_MEDIA.ID_EXT_MEDIA
        #    LEFT JOIN (
		#		 SELECT EXT_IMAGE.ID_EXT_MEDIA, PUB_CONFIG.MEDIA_VOD_URL AS THUMBNAIL_MEDIA_VOD_URL,EXT_SUPPORT.PATH AS THUMBNAIL_PATH,EXT_IMAGE.NAME AS THUMBNAIL_NAME,EXT_IMAGE.TIME_CODE,EXT_IMAGE.WIDTH AS IWIDTH,EXT_IMAGE.HEIGHT AS IHEIGHT,EXT_IMAGE.IMAGE_SIZE,EXT_IMAGE_TYPE.NAME AS ITYPE
		#		,EXT_SERVER.ID_EXT_SERVER,EXT_IMAGE.ID_EXT_IMAGE
		#		FROM EXT_MEDIA, EXT_SUPPORT,EXT_SERVER,EXT_IMAGE,EXT_IMAGE_TYPE, PUB_CONFIG
		#		WHERE EXT_MEDIA.ID_EXT_MEDIA = EXT_IMAGE.ID_EXT_MEDIA
        #        AND PUB_CONFIG.ID_EXT_SERVER = EXT_SERVER.ID_EXT_SERVER
		#		AND EXT_IMAGE.ID_EXT_SUPPORT= EXT_SUPPORT.ID_EXT_SUPPORT
		#		AND EXT_SUPPORT.ID_EXT_SERVER=EXT_SERVER.ID_EXT_SERVER
        #        AND EXT_MEDIA.ASSET_ID = %s
		#		AND EXT_IMAGE.ID_EXT_IMAGE_TYPE = %s
        #    )THUMBNAIL_QUERY
        #    ON THUMBNAIL_QUERY.ID_EXT_MEDIA = EXT_MEDIA.ID_EXT_MEDIA
        #    LEFT JOIN (
		#		 SELECT EXT_IMAGE.ID_EXT_MEDIA, PUB_CONFIG.MEDIA_VOD_URL AS STILL_MEDIA_VOD_URL,EXT_SUPPORT.PATH AS STILL_PATH,EXT_IMAGE.NAME AS STILL_NAME,EXT_IMAGE.TIME_CODE,EXT_IMAGE.WIDTH AS IWIDTH,EXT_IMAGE.HEIGHT AS IHEIGHT,EXT_IMAGE.IMAGE_SIZE,EXT_IMAGE_TYPE.NAME AS ITYPE
		#		,EXT_SERVER.ID_EXT_SERVER,EXT_IMAGE.ID_EXT_IMAGE
		#		FROM EXT_MEDIA, EXT_SUPPORT,EXT_SERVER,EXT_IMAGE,EXT_IMAGE_TYPE, PUB_CONFIG
		#		WHERE EXT_MEDIA.ID_EXT_MEDIA = EXT_IMAGE.ID_EXT_MEDIA
        #        AND PUB_CONFIG.ID_EXT_SERVER = EXT_SERVER.ID_EXT_SERVER
		#		AND EXT_IMAGE.ID_EXT_SUPPORT= EXT_SUPPORT.ID_EXT_SUPPORT
		#		AND EXT_SUPPORT.ID_EXT_SERVER=EXT_SERVER.ID_EXT_SERVER
        #        AND EXT_MEDIA.ASSET_ID = %s
		#		AND EXT_IMAGE.ID_EXT_IMAGE_TYPE = %s
        #    )STILL_QUERY
        #    ON STILL_QUERY.ID_EXT_MEDIA = EXT_MEDIA.ID_EXT_MEDIA
        #    LEFT JOIN (
		#		 SELECT EXT_IMAGE.ID_EXT_MEDIA, PUB_CONFIG.MEDIA_VOD_URL AS SPRITE_MEDIA_VOD_URL,EXT_SUPPORT.PATH AS SPRITE_PATH,EXT_IMAGE.NAME AS SPRITE_NAME,EXT_IMAGE.TIME_CODE,EXT_IMAGE.WIDTH AS IWIDTH,EXT_IMAGE.HEIGHT AS IHEIGHT,EXT_IMAGE.IMAGE_SIZE,EXT_IMAGE_TYPE.NAME AS ITYPE
		#		,EXT_SERVER.ID_EXT_SERVER,EXT_IMAGE.ID_EXT_IMAGE
		#		FROM EXT_MEDIA,EXT_SUPPORT,EXT_SERVER,EXT_IMAGE,EXT_IMAGE_TYPE, PUB_CONFIG
		#		WHERE EXT_MEDIA.ID_EXT_MEDIA = EXT_IMAGE.ID_EXT_MEDIA
        #        AND PUB_CONFIG.ID_EXT_SERVER = EXT_SERVER.ID_EXT_SERVER
		#		AND EXT_IMAGE.ID_EXT_SUPPORT= EXT_SUPPORT.ID_EXT_SUPPORT
		#		AND EXT_SUPPORT.ID_EXT_SERVER=EXT_SERVER.ID_EXT_SERVER
        #        AND EXT_MEDIA.ASSET_ID = %s
		#		AND EXT_IMAGE.ID_EXT_IMAGE_TYPE = %s
        #    )SPRITE_QUERY
        #    ON SPRITE_QUERY.ID_EXT_MEDIA = EXT_MEDIA.ID_EXT_MEDIA
        #    WHERE (EXT_MEDIA.ID_EXT_MEDIA=EXT_MEDIA.ID) 
        #    AND EXT_MEDIA.ASSET_ID = %s
        #    AND USERS.E_CORREO = %s""", (_id, 2, _id, 1, _id, 2, _id, 3, _id, 2, _id, 1, _id, _email))
        #results = self.cur.fetchall()  --> usamos mejor el cursor        
        result = []        

        for (_id_ext_media, _id, _asset_id, _asset_value, _f1_id, _f2_id, _c1, _c2, _f1, _f2, _c1_id, _c2_id, _title_10, _duration, _script_description, _remarks, _nescaleta, _nnoticia, _anyo_esc, _nombre_prog, _off, _synopsis_sar, _product_code
        , _chapter, _media_uti_cre, _media_uti_mod, _media_cre, _media_mod, _id_ext_media_type, _mp43_media_vodurl, _mp43_path, _mp43_filenameext, _webm_media_vodurl, _webm_path, _webm_filenameext, _hls_rendition_url, _thumbnail_media_vod_url
        , _thumbnail_path, _thumbnail_name, _still_media_vod_url, _still_path, _still_name, _sprite_media_vod_url, _sprite_path, _sprite_name) in self.cur:
            mp4_pmd_url = ""
            webm_url = ""
            thumbnail_url = ""
            still_url = ""
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

            if _still_media_vod_url:
                if _still_path:
                    if _still_name:                      
                       still_url = _still_media_vod_url + _still_path + _still_name
                       still_url = still_url.replace("http://","https://")
                       
            if _sprite_media_vod_url:
                if _sprite_path:
                    if _sprite_name:                      
                       sprite_url = _sprite_media_vod_url + _sprite_path + _sprite_name
                       sprite_url = sprite_url.replace("http://","https://")

            ext_media_type = "VIDEO"
            if _id_ext_media_type == 2:
                ext_media_type = "AUDIO"
            if _id_ext_media_type == 3:
                ext_media_type = "IMAGEN"

            _title_10 = self.funciones.FiltrarCaracteres(_title_10) 
            _script_description = self.funciones.FiltrarCaracteres(_script_description) 
            _remarks = self.funciones.FiltrarCaracteres(_remarks) 
            _off = self.funciones.FiltrarCaracteres(_off) 
            _synopsis_sar = self.funciones.FiltrarCaracteres(_synopsis_sar)    

            if _extended:
                new_row = {
                    "ID_EXT_MEDIA": _id_ext_media
                    , "ASSET_ID": _asset_id
                    , "ASSET_VALUE": _asset_value.strftime('%Y-%m-%d-%H:%M:%S')
                    , "F1_ID": _f1_id
                    , "F2_ID": _f2_id
                    , "C1_ID": _c1_id
                    , "C2_ID": _c2_id
                    , "F1": _f1
                    , "F2": _f2
                    , "C1": _c1
                    , "C2": _c2
                    , "TITLE_10": _title_10
                    , "DURATION": _duration
                    , "SCRIPT_DESCRIPTION": _script_description
                    , "REMARKS": _remarks
                    , "NESCALETA": _nescaleta
                    , "NNOTICIA": _nnoticia
                    , "ANYO_ESC": _anyo_esc
                    , "NOMBRE_PROG": _nombre_prog
                    , "OFF": _off
                    , "SYNOPSIS_SAR": _synopsis_sar
                    , "PRODUCT_CODE": _product_code
                    , "CHAPTER": _chapter
                    , "MEDIA_CRE": _media_cre.strftime('%Y-%m-%d-%H:%M:%S')
                    , "MEDIA_MOD": _media_mod.strftime('%Y-%m-%d-%H:%M:%S')
                    , "ID_EXT_MEDIA_TYPE": _id_ext_media_type
                    , "EXT_MEDIA_TYPE": ext_media_type 
                    , "HLS_RENDITION_URL": _hls_rendition_url
                    , "MP4PMD_URL": mp4_pmd_url
                    , "WEBMPMD_URL": webm_url
                    , "THUMBNAIL_URL": thumbnail_url
                    , "STILL_URL": still_url
                    , "SPRITE_URL": sprite_url             
                }
                #result.append(row)
                result = new_row
            else:
                new_row = {
                    "ID_EXT_MEDIA": _id_ext_media
                    , "ASSET_ID": _asset_id
                    , "ASSET_VALUE": _asset_value              
                    , "TITLE_10": _title_10
                    , "DURATION": _duration                  
                    , "EXT_MEDIA_TYPE": ext_media_type
                    , "HLS_RENDITION_URL": _hls_rendition_url
                    , "MP4PMD_URL": mp4_pmd_url
                    , "WEBMPMD_URL": webm_url
                    , "THUMBNAIL_URL": thumbnail_url
                    , "STILL_URL": still_url
                    , "SPRITE_URL": sprite_url             
                }
                #result.append(row)
                result = new_row
            break
        self.cur.close()
        return result    

    def ConsultaExtMediaFullByAssetId_pymysql_old(self,_id,_email,_extended):     
        self.cur.execute("""SELECT distinct EXT_MEDIA.ID_EXT_MEDIA,EXT_MEDIA.ID,EXT_MEDIA.ASSET_ID,DATE_FORMAT(EXT_MEDIA.ASSET_VALUE, '%%Y-%%m-%%d %%H:%%i:%%s') AS ASSET_VALUE
			,EXT_MEDIA.FORMATO1_ID AS F1_ID,EXT_MEDIA.FORMATO2_ID AS F2_ID,CONTENIDO1.CONTENIDO1_EUS AS C1,CONTENIDO2.CONTENIDO2_EUS AS C2,FORMATO1.FORMATO1_EUS AS F1,FORMATO2.FORMATO2_EUS AS F2
            ,EXT_MEDIA.CONTENIDO1_ID AS C1_ID,EXT_MEDIA.CONTENIDO2_ID AS C2_ID
            ,EXT_MEDIA.TITLE_10,EXT_MEDIA.DURATION,EXT_MEDIA.SCRIPT_DESCRIPTION,EXT_MEDIA.REMARKS,EXT_MEDIA.NESCALETA,EXT_MEDIA.NNOTICIA,EXT_MEDIA.ANYO_ESC,EXT_MEDIA.NOMBRE_PROG,EXT_MEDIA.OFF,EXT_MEDIA.SYNOPSIS_SAR,EXT_MEDIA.PRODUCT_CODE
			,EXT_MEDIA.CHAPTER,EXT_MEDIA.MEDIA_UTI_CRE,EXT_MEDIA.MEDIA_UTI_MOD,DATE_FORMAT(EXT_MEDIA.MEDIA_CRE, '%%Y-%%m-%%d %%H:%%i:%%s') AS MEDIA_CRE,DATE_FORMAT(EXT_MEDIA.MEDIA_MOD, '%%Y-%%m-%%d %%H:%%i:%%s') AS MEDIA_MOD,EXT_MEDIA.ID_EXT_MEDIA_TYPE			
            , MP43_QUERY.MP43_MEDIA_VODURL, MP43_QUERY.MP43_PATH, MP43_QUERY.MP43_FILENAMEEXT, WEBM_QUERY.WEBM_MEDIA_VODURL, WEBM_QUERY.WEBM_PATH, WEBM_QUERY.WEBM_FILENAMEEXT, HLS_QUERY.HLS_RENDITION_URL
            ,THUMBNAIL_QUERY.THUMBNAIL_MEDIA_VOD_URL, THUMBNAIL_QUERY.THUMBNAIL_PATH, THUMBNAIL_QUERY.THUMBNAIL_NAME,STILL_QUERY.STILL_MEDIA_VOD_URL, STILL_QUERY.STILL_PATH, STILL_QUERY.STILL_NAME, SPRITE_QUERY.SPRITE_MEDIA_VOD_URL, SPRITE_QUERY.SPRITE_PATH, SPRITE_QUERY.SPRITE_NAME
			FROM EXT_MEDIA 			
            LEFT JOIN FORMATO2 ON (EXT_MEDIA.FORMATO1_ID=FORMATO2.FORMATO1_ID AND EXT_MEDIA.FORMATO2_ID=FORMATO2.FORMATO2_ID) 
            LEFT JOIN FORMATO1 ON EXT_MEDIA.FORMATO1_ID=FORMATO1.FORMATO1_ID 
            LEFT JOIN CONTENIDO2 ON (EXT_MEDIA.CONTENIDO1_ID=CONTENIDO2.CONTENIDO1_ID AND EXT_MEDIA.CONTENIDO2_ID=CONTENIDO2.CONTENIDO2_ID) 
            LEFT JOIN CONTENIDO1 ON EXT_MEDIA.CONTENIDO1_ID=CONTENIDO1.CONTENIDO1_ID 					
            LEFT JOIN (
                   SELECT EXT_QUALITY.ID_EXT_MEDIA,EXT_QUALITY.FRAME_WIDTH,EXT_QUALITY.FRAME_HEIGHT,PUB_CONFIG.VOD_URL,
					PUB_CONFIG.MEDIA_VOD_URL AS WEBM_MEDIA_VODURL,EXT_SUPPORT.PATH AS WEBM_PATH,EXT_QUALITY.NAME AS WEBM_FILENAMEEXT,EXT_QUALITY.ENCODING_RATE
					FROM EXT_MEDIA,EXT_QUALITY, EXT_SUPPORT, EXT_SERVER, PUB_CONFIG
					WHERE EXT_MEDIA.ID_EXT_MEDIA = EXT_QUALITY.ID_EXT_MEDIA
                    AND PUB_CONFIG.ID_EXT_SERVER = EXT_SERVER.ID_EXT_SERVER
					AND EXT_SERVER.ID_EXT_SERVER = EXT_SUPPORT.ID_EXT_SERVER
					AND EXT_SUPPORT.ID_EXT_SUPPORT = EXT_QUALITY.ID_EXT_SUPPORT
                    AND EXT_MEDIA.ASSET_ID = %s
                    AND EXT_QUALITY.ID_EXT_QUALITY_TYPE = %s
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
					AND EXT_QUALITY.ID_EXT_QUALITY_TYPE = %s
			) MP43_QUERY
		    ON MP43_QUERY.ID_EXT_MEDIA = EXT_MEDIA.ID_EXT_MEDIA
            LEFT JOIN (
				 SELECT CMS_RENDITION.RENDITION_URL AS HLS_RENDITION_URL, EXT_QUALITY_CMS_RENDITION.ID_EXT_MEDIA                      
				 from  EXT_MEDIA, CMS_RENDITION, EXT_QUALITY_CMS_RENDITION              
				 where EXT_MEDIA.ID_EXT_MEDIA = EXT_QUALITY_CMS_RENDITION.ID_EXT_MEDIA
                 AND CMS_RENDITION.ID_CMS_RENDITION=EXT_QUALITY_CMS_RENDITION.ID_CMS_RENDITION
                 AND EXT_MEDIA.ASSET_ID = %s
				 AND CMS_RENDITION.ID_PUBLICATION_TYPE = %s
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
				AND EXT_IMAGE.ID_EXT_IMAGE_TYPE = %s
            )THUMBNAIL_QUERY
            ON THUMBNAIL_QUERY.ID_EXT_MEDIA = EXT_MEDIA.ID_EXT_MEDIA
            LEFT JOIN (
				 SELECT EXT_IMAGE.ID_EXT_MEDIA, PUB_CONFIG.MEDIA_VOD_URL AS STILL_MEDIA_VOD_URL,EXT_SUPPORT.PATH AS STILL_PATH,EXT_IMAGE.NAME AS STILL_NAME,EXT_IMAGE.TIME_CODE,EXT_IMAGE.WIDTH AS IWIDTH,EXT_IMAGE.HEIGHT AS IHEIGHT,EXT_IMAGE.IMAGE_SIZE,EXT_IMAGE_TYPE.NAME AS ITYPE
				,EXT_SERVER.ID_EXT_SERVER,EXT_IMAGE.ID_EXT_IMAGE
				FROM EXT_MEDIA, EXT_SUPPORT,EXT_SERVER,EXT_IMAGE,EXT_IMAGE_TYPE, PUB_CONFIG
				WHERE EXT_MEDIA.ID_EXT_MEDIA = EXT_IMAGE.ID_EXT_MEDIA
                AND PUB_CONFIG.ID_EXT_SERVER = EXT_SERVER.ID_EXT_SERVER
				AND EXT_IMAGE.ID_EXT_SUPPORT= EXT_SUPPORT.ID_EXT_SUPPORT
				AND EXT_SUPPORT.ID_EXT_SERVER=EXT_SERVER.ID_EXT_SERVER
                AND EXT_MEDIA.ASSET_ID = %s
				AND EXT_IMAGE.ID_EXT_IMAGE_TYPE = %s
            )STILL_QUERY
            ON STILL_QUERY.ID_EXT_MEDIA = EXT_MEDIA.ID_EXT_MEDIA
            LEFT JOIN (
				 SELECT EXT_IMAGE.ID_EXT_MEDIA, PUB_CONFIG.MEDIA_VOD_URL AS SPRITE_MEDIA_VOD_URL,EXT_SUPPORT.PATH AS SPRITE_PATH,EXT_IMAGE.NAME AS SPRITE_NAME,EXT_IMAGE.TIME_CODE,EXT_IMAGE.WIDTH AS IWIDTH,EXT_IMAGE.HEIGHT AS IHEIGHT,EXT_IMAGE.IMAGE_SIZE,EXT_IMAGE_TYPE.NAME AS ITYPE
				,EXT_SERVER.ID_EXT_SERVER,EXT_IMAGE.ID_EXT_IMAGE
				FROM EXT_MEDIA,EXT_SUPPORT,EXT_SERVER,EXT_IMAGE,EXT_IMAGE_TYPE, PUB_CONFIG
				WHERE EXT_MEDIA.ID_EXT_MEDIA = EXT_IMAGE.ID_EXT_MEDIA
                AND PUB_CONFIG.ID_EXT_SERVER = EXT_SERVER.ID_EXT_SERVER
				AND EXT_IMAGE.ID_EXT_SUPPORT= EXT_SUPPORT.ID_EXT_SUPPORT
				AND EXT_SUPPORT.ID_EXT_SERVER=EXT_SERVER.ID_EXT_SERVER
                AND EXT_MEDIA.ASSET_ID = %s
				AND EXT_IMAGE.ID_EXT_IMAGE_TYPE = %s
            )SPRITE_QUERY
            ON SPRITE_QUERY.ID_EXT_MEDIA = EXT_MEDIA.ID_EXT_MEDIA               
            WHERE EXT_MEDIA.ID is not null            
			AND EXT_MEDIA.ASSET_ID = %s
            AND EXT_MEDIA.ASSET_ID IN (
				SELECT ASSET_ID FROM PROJECT_ASSET
				INNER JOIN NODE_GROUPUSER_MEDIA ON NODE_GROUPUSER_MEDIA.PROJECT_ID = PROJECT_ASSET.PROJECT_ID
				INNER JOIN GROUPUSER_USERS ON GROUPUSER_USERS.GROUP_USER_ID = NODE_GROUPUSER_MEDIA.GROUP_USER_ID
				INNER JOIN USERS ON USERS.USER_ID = GROUPUSER_USERS.USER_ID			
				WHERE USERS.E_CORREO = %s         
            )""", (_id, 2, _id, 1, _id, 2, _id, 3, _id, 2, _id, 1, _id, _email))

        #self.cur.execute("""SELECT distinct EXT_MEDIA.ID_EXT_MEDIA,EXT_MEDIA.ID,EXT_MEDIA.ASSET_ID,DATE_FORMAT(EXT_MEDIA.ASSET_VALUE, '%%Y-%%m-%%d %%H:%%i:%%s') AS ASSET_VALUE,EXT_MEDIA.FORMATO1_ID AS F1_ID,EXT_MEDIA.FORMATO2_ID AS F2_ID,CONTENIDO1.CONTENIDO1_EUS AS C1,CONTENIDO2.CONTENIDO2_EUS AS C2,FORMATO1.FORMATO1_EUS AS F1,FORMATO2.FORMATO2_EUS AS F2,
        #    EXT_MEDIA.CONTENIDO1_ID AS C1_ID,EXT_MEDIA.CONTENIDO2_ID AS C2_ID,EXT_MEDIA.TITLE_10,EXT_MEDIA.DURATION,EXT_MEDIA.SCRIPT_DESCRIPTION,EXT_MEDIA.REMARKS,EXT_MEDIA.NESCALETA,EXT_MEDIA.NNOTICIA,EXT_MEDIA.ANYO_ESC,EXT_MEDIA.NOMBRE_PROG,EXT_MEDIA.OFF,EXT_MEDIA.SYNOPSIS_SAR,EXT_MEDIA.PRODUCT_CODE
		#    ,EXT_MEDIA.CHAPTER,EXT_MEDIA.MEDIA_UTI_CRE,EXT_MEDIA.MEDIA_UTI_MOD,DATE_FORMAT(EXT_MEDIA.MEDIA_CRE, '%%Y-%%m-%%d %%H:%%i:%%s') AS MEDIA_CRE,DATE_FORMAT(EXT_MEDIA.MEDIA_MOD, '%%Y-%%m-%%d %%H:%%i:%%s') AS MEDIA_MOD,EXT_MEDIA.ID_EXT_MEDIA_TYPE
		#	,EXT_MEDIA_TYPE.NAME AS EXT_MEDIA_TYPE, MP43_QUERY.MP43_MEDIA_VODURL, MP43_QUERY.MP43_PATH, MP43_QUERY.MP43_FILENAMEEXT, WEBM_QUERY.WEBM_MEDIA_VODURL, WEBM_QUERY.WEBM_PATH, WEBM_QUERY.WEBM_FILENAMEEXT, HLS_QUERY.HLS_RENDITION_URL
        #    ,THUMBNAIL_QUERY.THUMBNAIL_MEDIA_VOD_URL, THUMBNAIL_QUERY.THUMBNAIL_PATH, THUMBNAIL_QUERY.THUMBNAIL_NAME,STILL_QUERY.STILL_MEDIA_VOD_URL, STILL_QUERY.STILL_PATH, STILL_QUERY.STILL_NAME, SPRITE_QUERY.SPRITE_MEDIA_VOD_URL, SPRITE_QUERY.SPRITE_PATH, SPRITE_QUERY.SPRITE_NAME
		#	FROM EXT_MEDIA 
        #    JOIN PROJECT_ASSET ON EXT_MEDIA.ASSET_ID = PROJECT_ASSET.ASSET_ID
        #    JOIN NODE_GROUPUSER_MEDIA ON NODE_GROUPUSER_MEDIA.PROJECT_ID = PROJECT_ASSET.PROJECT_ID
        #    JOIN GROUPUSER_USERS ON GROUPUSER_USERS.GROUP_USER_ID = NODE_GROUPUSER_MEDIA.GROUP_USER_ID
        #    JOIN USERS ON USERS.USER_ID = GROUPUSER_USERS.USER_ID
        #    LEFT JOIN FORMATO2 ON (EXT_MEDIA.FORMATO1_ID=FORMATO2.FORMATO1_ID AND EXT_MEDIA.FORMATO2_ID=FORMATO2.FORMATO2_ID) 
        #    LEFT JOIN FORMATO1 ON EXT_MEDIA.FORMATO1_ID=FORMATO1.FORMATO1_ID 
        #    LEFT JOIN CONTENIDO2 ON (EXT_MEDIA.CONTENIDO1_ID=CONTENIDO2.CONTENIDO1_ID AND EXT_MEDIA.CONTENIDO2_ID=CONTENIDO2.CONTENIDO2_ID) 
        #    LEFT JOIN CONTENIDO1 ON EXT_MEDIA.CONTENIDO1_ID=CONTENIDO1.CONTENIDO1_ID 
		#	LEFT JOIN EXT_MEDIA_TYPE ON EXT_MEDIA_TYPE.ID_EXT_MEDIA_TYPE = EXT_MEDIA.ID_EXT_MEDIA_TYPE   
        #    LEFT JOIN (
        #           SELECT EXT_QUALITY.ID_EXT_MEDIA,EXT_QUALITY.FRAME_WIDTH,EXT_QUALITY.FRAME_HEIGHT,PUB_CONFIG.VOD_URL,
		#			PUB_CONFIG.MEDIA_VOD_URL AS WEBM_MEDIA_VODURL,EXT_SUPPORT.PATH AS WEBM_PATH,EXT_QUALITY.NAME AS WEBM_FILENAMEEXT,EXT_QUALITY.ENCODING_RATE
		#			FROM EXT_MEDIA,EXT_QUALITY, EXT_SUPPORT, EXT_SERVER, PUB_CONFIG
		#			WHERE EXT_MEDIA.ID_EXT_MEDIA = EXT_QUALITY.ID_EXT_MEDIA
        #            AND PUB_CONFIG.ID_EXT_SERVER = EXT_SERVER.ID_EXT_SERVER
		#			AND EXT_SERVER.ID_EXT_SERVER = EXT_SUPPORT.ID_EXT_SERVER
		#			AND EXT_SUPPORT.ID_EXT_SUPPORT = EXT_QUALITY.ID_EXT_SUPPORT
        #            AND EXT_MEDIA.ASSET_ID = %s
        #            AND EXT_QUALITY.ID_EXT_QUALITY_TYPE = %s
		#	) WEBM_QUERY
		#    ON WEBM_QUERY.ID_EXT_MEDIA = EXT_MEDIA.ID_EXT_MEDIA
        #    LEFT JOIN (
        #           SELECT EXT_QUALITY.ID_EXT_MEDIA,EXT_QUALITY.FRAME_WIDTH,EXT_QUALITY.FRAME_HEIGHT,PUB_CONFIG.VOD_URL,
		#			PUB_CONFIG.MEDIA_VOD_URL AS MP43_MEDIA_VODURL,EXT_SUPPORT.PATH AS MP43_PATH,EXT_QUALITY.NAME AS MP43_FILENAMEEXT,EXT_QUALITY.ENCODING_RATE
		#			FROM EXT_MEDIA, EXT_QUALITY, EXT_SUPPORT, EXT_SERVER, PUB_CONFIG
		#			WHERE EXT_MEDIA.ID_EXT_MEDIA = EXT_QUALITY.ID_EXT_MEDIA
        #            AND PUB_CONFIG.ID_EXT_SERVER = EXT_SERVER.ID_EXT_SERVER
		#			AND EXT_SERVER.ID_EXT_SERVER = EXT_SUPPORT.ID_EXT_SERVER
		#			AND EXT_SUPPORT.ID_EXT_SUPPORT = EXT_QUALITY.ID_EXT_SUPPORT
        #            AND EXT_MEDIA.ASSET_ID = %s
		#			AND EXT_QUALITY.ID_EXT_QUALITY_TYPE = %s
		#	) MP43_QUERY
		#    ON MP43_QUERY.ID_EXT_MEDIA = EXT_MEDIA.ID_EXT_MEDIA
        #    LEFT JOIN (
		#		 SELECT CMS_RENDITION.RENDITION_URL AS HLS_RENDITION_URL, EXT_QUALITY_CMS_RENDITION.ID_EXT_MEDIA                      
		#		 from  EXT_MEDIA, CMS_RENDITION, EXT_QUALITY_CMS_RENDITION              
		#		 where EXT_MEDIA.ID_EXT_MEDIA = EXT_QUALITY_CMS_RENDITION.ID_EXT_MEDIA
        #         AND CMS_RENDITION.ID_CMS_RENDITION=EXT_QUALITY_CMS_RENDITION.ID_CMS_RENDITION
        #         AND EXT_MEDIA.ASSET_ID = %s
		#		 AND CMS_RENDITION.ID_PUBLICATION_TYPE = %s
        #    ) HLS_QUERY
        #    ON HLS_QUERY.ID_EXT_MEDIA = EXT_MEDIA.ID_EXT_MEDIA
        #    LEFT JOIN (
		#		 SELECT EXT_IMAGE.ID_EXT_MEDIA, PUB_CONFIG.MEDIA_VOD_URL AS THUMBNAIL_MEDIA_VOD_URL,EXT_SUPPORT.PATH AS THUMBNAIL_PATH,EXT_IMAGE.NAME AS THUMBNAIL_NAME,EXT_IMAGE.TIME_CODE,EXT_IMAGE.WIDTH AS IWIDTH,EXT_IMAGE.HEIGHT AS IHEIGHT,EXT_IMAGE.IMAGE_SIZE,EXT_IMAGE_TYPE.NAME AS ITYPE
		#		,EXT_SERVER.ID_EXT_SERVER,EXT_IMAGE.ID_EXT_IMAGE
		#		FROM EXT_MEDIA, EXT_SUPPORT,EXT_SERVER,EXT_IMAGE,EXT_IMAGE_TYPE, PUB_CONFIG
		#		WHERE EXT_MEDIA.ID_EXT_MEDIA = EXT_IMAGE.ID_EXT_MEDIA
        #        AND PUB_CONFIG.ID_EXT_SERVER = EXT_SERVER.ID_EXT_SERVER
		#		AND EXT_IMAGE.ID_EXT_SUPPORT= EXT_SUPPORT.ID_EXT_SUPPORT
		#		AND EXT_SUPPORT.ID_EXT_SERVER=EXT_SERVER.ID_EXT_SERVER
        #        AND EXT_MEDIA.ASSET_ID = %s
		#		AND EXT_IMAGE.ID_EXT_IMAGE_TYPE = %s
        #    )THUMBNAIL_QUERY
        #    ON THUMBNAIL_QUERY.ID_EXT_MEDIA = EXT_MEDIA.ID_EXT_MEDIA
        #    LEFT JOIN (
		#		 SELECT EXT_IMAGE.ID_EXT_MEDIA, PUB_CONFIG.MEDIA_VOD_URL AS STILL_MEDIA_VOD_URL,EXT_SUPPORT.PATH AS STILL_PATH,EXT_IMAGE.NAME AS STILL_NAME,EXT_IMAGE.TIME_CODE,EXT_IMAGE.WIDTH AS IWIDTH,EXT_IMAGE.HEIGHT AS IHEIGHT,EXT_IMAGE.IMAGE_SIZE,EXT_IMAGE_TYPE.NAME AS ITYPE
		#		,EXT_SERVER.ID_EXT_SERVER,EXT_IMAGE.ID_EXT_IMAGE
		#		FROM EXT_MEDIA, EXT_SUPPORT,EXT_SERVER,EXT_IMAGE,EXT_IMAGE_TYPE, PUB_CONFIG
		#		WHERE EXT_MEDIA.ID_EXT_MEDIA = EXT_IMAGE.ID_EXT_MEDIA
        #        AND PUB_CONFIG.ID_EXT_SERVER = EXT_SERVER.ID_EXT_SERVER
		#		AND EXT_IMAGE.ID_EXT_SUPPORT= EXT_SUPPORT.ID_EXT_SUPPORT
		#		AND EXT_SUPPORT.ID_EXT_SERVER=EXT_SERVER.ID_EXT_SERVER
        #        AND EXT_MEDIA.ASSET_ID = %s
		#		AND EXT_IMAGE.ID_EXT_IMAGE_TYPE = %s
        #    )STILL_QUERY
        #    ON STILL_QUERY.ID_EXT_MEDIA = EXT_MEDIA.ID_EXT_MEDIA
        #    LEFT JOIN (
		#		 SELECT EXT_IMAGE.ID_EXT_MEDIA, PUB_CONFIG.MEDIA_VOD_URL AS SPRITE_MEDIA_VOD_URL,EXT_SUPPORT.PATH AS SPRITE_PATH,EXT_IMAGE.NAME AS SPRITE_NAME,EXT_IMAGE.TIME_CODE,EXT_IMAGE.WIDTH AS IWIDTH,EXT_IMAGE.HEIGHT AS IHEIGHT,EXT_IMAGE.IMAGE_SIZE,EXT_IMAGE_TYPE.NAME AS ITYPE
		#		,EXT_SERVER.ID_EXT_SERVER,EXT_IMAGE.ID_EXT_IMAGE
		#		FROM EXT_MEDIA,EXT_SUPPORT,EXT_SERVER,EXT_IMAGE,EXT_IMAGE_TYPE, PUB_CONFIG
		#		WHERE EXT_MEDIA.ID_EXT_MEDIA = EXT_IMAGE.ID_EXT_MEDIA
        #        AND PUB_CONFIG.ID_EXT_SERVER = EXT_SERVER.ID_EXT_SERVER
		#		AND EXT_IMAGE.ID_EXT_SUPPORT= EXT_SUPPORT.ID_EXT_SUPPORT
		#		AND EXT_SUPPORT.ID_EXT_SERVER=EXT_SERVER.ID_EXT_SERVER
        #        AND EXT_MEDIA.ASSET_ID = %s
		#		AND EXT_IMAGE.ID_EXT_IMAGE_TYPE = %s
        #    )SPRITE_QUERY
        #    ON SPRITE_QUERY.ID_EXT_MEDIA = EXT_MEDIA.ID_EXT_MEDIA
        #    WHERE (EXT_MEDIA.ID_EXT_MEDIA=EXT_MEDIA.ID) 
        #    AND EXT_MEDIA.ASSET_ID = %s
        #    AND USERS.E_CORREO = %s""", (_id, 2, _id, 1, _id, 2, _id, 3, _id, 2, _id, 1, _id, _email))
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
            _still_media_vod_url = row['STILL_MEDIA_VOD_URL']
            _still_path = row['STILL_PATH']
            _still_name = row['STILL_NAME']                       
            _sprite_media_vod_url = row['SPRITE_MEDIA_VOD_URL']
            _sprite_path = row['SPRITE_PATH']
            _sprite_name = row['SPRITE_NAME']                       
            
            mp4_pmd_url = ""
            webm_url = ""
            thumbnail_url = ""
            still_url = ""
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

            if _still_media_vod_url:
                if _still_path:
                    if _still_name:                      
                       still_url = _still_media_vod_url + _still_path + _still_name
                       still_url = still_url.replace("http://","https://")
                       
            if _sprite_media_vod_url:
                if _sprite_path:
                    if _sprite_name:                      
                       sprite_url = _sprite_media_vod_url + _sprite_path + _sprite_name
                       sprite_url = sprite_url.replace("http://","https://")

            ext_media_type = "VIDEO"
            if row['ID_EXT_MEDIA_TYPE'] == 2:
                ext_media_type = "AUDIO"
            if row['ID_EXT_MEDIA_TYPE'] == 3:
                ext_media_type = "IMAGEN"
           
            row['MP4PMD_URL'] = mp4_pmd_url
            row['WEBMPMD_URL'] = webm_url            
            row['THUMBNAIL_URL'] = thumbnail_url
            row['STILL_URL'] = still_url
            row['SPRITE_URL'] = sprite_url

            row['TITLE_10'] = self.funciones.FiltrarCaracteres(row['TITLE_10']) 
            row['SCRIPT_DESCRIPTION'] = self.funciones.FiltrarCaracteres(row['SCRIPT_DESCRIPTION']) 
            row['REMARKS'] = self.funciones.FiltrarCaracteres(row['REMARKS']) 
            row['OFF'] = self.funciones.FiltrarCaracteres(row['OFF']) 
            row['SYNOPSIS_SAR'] = self.funciones.FiltrarCaracteres(row['SYNOPSIS_SAR'])        

            if _extended:
                new_row = {
                    "ID_EXT_MEDIA": row['ID_EXT_MEDIA']
                    , "ASSET_ID": row['ASSET_ID']
                    , "ASSET_VALUE": row['ASSET_VALUE']
                    , "F1_ID": row['F1_ID']
                    , "F2_ID": row['F2_ID']
                    , "C1_ID": row['C1_ID']
                    , "C2_ID": row['C2_ID']
                    , "F1": row['F1']
                    , "F2": row['F2']
                    , "C1": row['C1']
                    , "C2": row['C2']
                    , "TITLE_10": row['TITLE_10']
                    , "DURATION": row['DURATION']
                    , "SCRIPT_DESCRIPTION": row['SCRIPT_DESCRIPTION']
                    , "REMARKS": row['REMARKS']
                    , "NESCALETA": row['NESCALETA']
                    , "NNOTICIA": row['NNOTICIA']
                    , "ANYO_ESC": row['ANYO_ESC']
                    , "NOMBRE_PROG": row['NOMBRE_PROG']
                    , "OFF": row['OFF']
                    , "SYNOPSIS_SAR": row['SYNOPSIS_SAR']
                    , "PRODUCT_CODE": row['PRODUCT_CODE']
                    , "CHAPTER": row['CHAPTER']
                    , "MEDIA_CRE": row['MEDIA_CRE']
                    , "MEDIA_MOD": row['MEDIA_MOD']
                    , "ID_EXT_MEDIA_TYPE": row['ID_EXT_MEDIA_TYPE']
                    , "EXT_MEDIA_TYPE": ext_media_type 
                    , "HLS_RENDITION_URL": row['HLS_RENDITION_URL']
                    , "MP4PMD_URL": row['MP4PMD_URL']
                    , "WEBMPMD_URL": row['WEBMPMD_URL']
                    , "THUMBNAIL_URL": row['THUMBNAIL_URL']
                    , "STILL_URL": row['STILL_URL']
                    , "SPRITE_URL": row['SPRITE_URL']               
                }
                #result.append(row)
                result = new_row
            else:
                new_row = {
                    "ID_EXT_MEDIA": row['ID_EXT_MEDIA']
                    , "ASSET_ID": row['ASSET_ID']
                    , "ASSET_VALUE": row['ASSET_VALUE']                   
                    , "TITLE_10": row['TITLE_10']
                    , "DURATION": row['DURATION']                    
                    , "EXT_MEDIA_TYPE": ext_media_type
                    , "HLS_RENDITION_URL": row['HLS_RENDITION_URL']
                    , "MP4PMD_URL": row['MP4PMD_URL']
                    , "WEBMPMD_URL": row['WEBMPMD_URL']
                    , "THUMBNAIL_URL": row['THUMBNAIL_URL']
                    , "STILL_URL": row['STILL_URL']
                    , "SPRITE_URL": row['SPRITE_URL']               
                }
                #result.append(row)
                result = new_row
            break
        return result    

    

    def ConsultaProjectListByUser(self,_email):        
        self.cur.execute("""select PROJECT.PROJECT_ID, PROJECT.TITLE_01, DATE_FORMAT(NODE_GROUPUSER_MEDIA.NGM_CRE, '%%Y-%%m-%%d %%H:%%i:%%s') AS FECHA_CRE
            FROM PROJECT,NODE_GROUPUSER_MEDIA,GROUPUSER_USERS,USERS,GROUPUSER_FRAMEAPLICATION
            WHERE PROJECT.PROJECT_ID = NODE_GROUPUSER_MEDIA.PROJECT_ID
            AND NODE_GROUPUSER_MEDIA.GROUP_USER_ID = GROUPUSER_USERS.GROUP_USER_ID
            AND GROUPUSER_USERS.USER_ID = USERS.USER_ID 
            AND GROUPUSER_USERS.GROUP_USER_ID = GROUPUSER_FRAMEAPLICATION.GROUP_USER_ID
            AND GROUPUSER_FRAMEAPLICATION.FRAME_APLICATION_ID = 115
            AND USERS.E_CORREO = %s ORDER BY FECHA_CRE DESC""", _email)
        #results = self.cur.fetchall()  --> usamos mejor el cursor        
        result = []
        for row in self.cur:            
            row['PROJECT_ID'] = row['PROJECT_ID'] 
            row['TITLE_01'] = self.funciones.FiltrarCaracteres(row['TITLE_01'])       
            row['FECHA_CRE'] = row['FECHA_CRE']       
            result.append(row)    
        self.cur.close()        
        return result

    def ConsultaUserRightFramesAppByFrameAppUser(self,_frameAppid,_email):  
        self.cur.execute("""SELECT RIGHT_FRAMEAPLICATION.RIGHT_FRAMEAPLICATION_ID, RIGHT_FRAMEAPLICATION.FRAME_APLICATION_ID, RIGHT_FRAMEAPLICATION.NAME,RIGHT_FRAMEAPLICATION.WEB_URL,RIGHT_FRAMEAPLICATION.WEB_CLASS,RIGHT_FRAMEAPLICATION.WEB_TITLE 
        FROM RIGHT_FRAMEAPLICATION,GROUP_USER_RIGHTFRAMEAPP,GROUPUSER_USERS,USERS
        WHERE 
        RIGHT_FRAMEAPLICATION.RIGHT_FRAMEAPLICATION_ID = GROUP_USER_RIGHTFRAMEAPP.RIGHT_FRAMEAPLICATION_ID
        AND GROUP_USER_RIGHTFRAMEAPP.GROUP_USER_ID = GROUPUSER_USERS.GROUP_USER_ID
        AND GROUPUSER_USERS.USER_ID = USERS.USER_ID
        AND RIGHT_FRAMEAPLICATION.FRAME_APLICATION_ID = %s
        AND USERS.E_CORREO = %s""", (_frameAppid, _email))
        #results = self.cur.fetchall()  --> usamos mejor el cursor        
        result = []
        for row in self.cur:            
            row['RIGHT_FRAMEAPLICATION_ID'] = row['RIGHT_FRAMEAPLICATION_ID'] 
            row['FRAME_APLICATION_ID'] = row['FRAME_APLICATION_ID'] 
            row['NAME'] = self.funciones.FiltrarCaracteres(row['NAME'])       
            row['WEB_URL'] = row['WEB_URL']       
            row['WEB_CLASS'] = row['WEB_CLASS']    
            row['WEB_TITLE'] = row['WEB_TITLE']    
            result.append(row)   
        self.cur.close()
        return result


class Metodos:    
    def __init__(self):
        self.funciones = Funciones()            

    def funcion_GetAssetListByProjectId(self,_id,_email):        
        self.funciones.EscribeLog(settings.LOG_FILENAME, '{}{}{}{}'.format('INICIO funcion_GetAssetListByProjectId - _id: ', _id,' - _email:',_email), self.funciones._INFO)  
        db = Database()   
        db.Connect_pymysql()       
        asss = db.ConsultaAssetListByProjectId(_id,_email)  
        db.Disconnect()              
        self.funciones.EscribeLog(settings.LOG_FILENAME, '{}{}'.format('funcion_GetAssetListByProjectId - assets: ', str(asss)), self.funciones._INFO)    
        return asss
        
    def funcion_GetExtMediaByAssetId(self,_id,_email):
        self.funciones.EscribeLog(settings.LOG_FILENAME, '{}{}{}{}'.format('INICIO funcion_GetExtMediaByAssetId - _id: ', _id,' - _email:',_email), self.funciones._INFO)        
        db = Database()  
        db.Connect_pymysql()
        asss = db.ConsultaExtMediaByAssetId(_id, _email)    
        db.Disconnect()    
        self.funciones.EscribeLog(settings.LOG_FILENAME, '{}{}'.format('funcion_GetExtMediaByAssetId - ext_media: ', str(asss)), self.funciones._INFO)  
        return asss

    def funcion_GetExtMediaFullByAssetId(self,_id,_email,_extended):
        self.funciones.EscribeLog(settings.LOG_FILENAME, '{}{}{}{}{}{}'.format('INICIO funcion_GetExtMediaFullByAssetId - _id: ', _id, ' - _email:', _email, ' - extended:', _extended), self.funciones._INFO)          
        db = Database()  
        db.Connect_pymysql()
        asss = db.ConsultaExtMediaFullByAssetId_pymysql(_id,_email,_extended)        
        db.Disconnect()
        self.funciones.EscribeLog(settings.LOG_FILENAME, '{}{}'.format('funcion_GetExtMediaFullByAssetId - ext_media: ', str(asss)), self.funciones._INFO)  
        return asss

    def funcion_GetProjectListByUser(self,_email):        
        self.funciones.EscribeLog(settings.LOG_FILENAME, '{}{}'.format('INICIO funcion_GetProjectListByUser - _email: ', _email), self.funciones._INFO)  
        db = Database()  
        db.Connect_pymysql()
        proys = db.ConsultaProjectListByUser(_email)    
        db.Disconnect()    
        self.funciones.EscribeLog(settings.LOG_FILENAME, '{}{}'.format('funcion_GetProjectListByUser - proys: ', str(proys)), self.funciones._INFO)    
        return proys

    def funcion_GetUserRightFramesAppByFrameAppUser(self,_frameAppid,_email):        
        self.funciones.EscribeLog(settings.LOG_FILENAME, '{}{}{}{}'.format('INICIO funcion_GetUserRightFramesAppByFrameAppUser - _email: ', _email, ' - _frameAppid:',_frameAppid), self.funciones._INFO)  
        db = Database()  
        db.Connect_pymysql()
        rightFrameApps = db.ConsultaUserRightFramesAppByFrameAppUser(_frameAppid,_email)     
        db.Disconnect()   
        self.funciones.EscribeLog(settings.LOG_FILENAME, '{}{}'.format('funcion_GetUserRightFramesAppByFrameAppUser - rightFrameApps: ', str(rightFrameApps)), self.funciones._INFO)    
        return rightFrameApps
        