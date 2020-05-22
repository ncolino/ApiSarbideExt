"""The Endpoints to manage the BOOK_REQUESTS"""
import uuid
from datetime import datetime, timedelta
from flask import jsonify, abort, request, Blueprint

import hashlib 
import json

from custom import Database, Metodos

from validate_email import validate_email

import settings


REQUEST_API = Blueprint('request_api', __name__)



def get_blueprint():
    """Return the blueprint for the main app module"""  
    return REQUEST_API

def generate_etag(data):  #data debe ser un string y estar hay que hacerle .encode()
    """Generate an etag for some data."""
    return hashlib.md5(data).hexdigest()

@REQUEST_API.route('/', methods=['GET'])
def PaginaInicio():
    """Retorna la lista de assets para un proyecto dado    
    @return: 200: Página de inicio \
    with application/json mimetype.
    @raise 404: if pagina inicio not found
    """
    
    return 'Página de Inicio ' + settings.APP_HOSTNAME, 200


@REQUEST_API.route('/Asset/ByProject/<string:tag>/<string:project_id>/<string:email>', methods=['GET'])
def GetAssetListByProjectId(tag,project_id,email):
    """Retorna la lista de assets para un proyecto dado
    @param tag: the id    
    @param project_id: the id
    @param email: email del user
    @return: 200: a ASSET_REQUESTS as a flask/response object \
    with application/json mimetype.
    @raise 404: if asset not found
    """
    fn = Metodos() 
    res = fn.funcion_GetAssetListByProjectId(project_id,email)   
    if len(res) == 0:
       #abort(404)
       return '', 204

    resp = jsonify(res)
    resp.set_etag(generate_etag(json.dumps(res).encode()))
    return resp
    #return jsonify(res)
    

@REQUEST_API.route('/Asset/Project/<string:tag>/<string:project_id>/<string:email>/<string:page_number>/<string:page_size>', methods=['GET'])
def GetAssetListByProjectIdPaginated(tag,project_id,email,page_number,page_size):
    """Retorna la lista de assets para un proyecto dado
    @param tag: the id    
    @param project_id: the id
    @param email: email del user
    @param page_number: page_number
    @param page_size: page_size
    @return: 200: a ASSET_REQUESTS as a flask/response object \
    with application/json mimetype.
    @raise 404: if asset not found
    """
    fn = Metodos() 
    res = fn.funcion_GetAssetListByProjectIdPaginated(project_id,email,page_number,page_size)   
    if len(res) == 0:
       #abort(404)
       return '', 204

    resp = jsonify(res)
    resp.set_etag(generate_etag(json.dumps(res).encode()))
    return resp
    #return jsonify(res)
    


@REQUEST_API.route('/ExtMedia/Asset/<string:tag>/<string:asset_id>/<string:email>', methods=['GET'])
def GetExtMediaByAssetId(tag,asset_id,email):
    """Lectura del extmedia dado el asset
    @param tag: the id   
    @param asset_id: the id
    @param email: email del user    
    @return: 200: a EXTMEDIA_REQUESTS as a flask/response object \
    with application/json mimetype.
    @raise 404: if extmedia not found
    """
    fn = Metodos() 
    res = fn.funcion_GetExtMediaByAssetId(asset_id,email)   
    if len(res) == 0:
       #abort(404)
       return '', 204
    
    resp = jsonify(res)
    resp.set_etag(generate_etag(json.dumps(res).encode()))
    return resp
    #return jsonify(res)
    



@REQUEST_API.route('/ExtMedia/Asset/full/<string:tag>/<string:asset_id>/<string:email>', methods=['GET'])
def GetExtMediaFullByAssetId(tag,asset_id,email):
    """Lectura de toda la metadata de extmedia dado el asset
    @param tag: the id   
    @param asset_id: the id
    @param email: email del user   
    @return: 200: a EXTMEDIA_REQUESTS as a flask/response object \
    with application/json mimetype.
    @raise 404: if extmedia not found
    """
    extended = True
    fn = Metodos() 
    res = fn.funcion_GetExtMediaFullByAssetId(asset_id, email, extended)   
    if len(res) == 0:
       #abort(404)
       return '', 204
    
    resp = jsonify(res)
    resp.set_etag(generate_etag(json.dumps(res).encode()))
    return resp
    #return jsonify(res)
    


@REQUEST_API.route('/ExtMedia/Project/<string:tag>/<string:project_id>/<string:email>', methods=['GET'])
def GetExtMediasByProjectId(tag,project_id,email):
    """Lectura de toda la metadata de los extmedia dado el proyecto
    @param tag: the id   
    @param project_id: the id
    @param email: email del user
    @return: 200: a EXTMEDIA_REQUESTS as a flask/response object \
    with application/json mimetype.
    @raise 404: if extmedia not found
    """
    extended = None
    fn = Metodos() 
    res = fn.funcion_GetAssetListByProjectId(project_id, email)  
    result = []
    if len(res) > 0:        
        for asset_id in res:            
            res_asset = fn.funcion_GetExtMediaFullByAssetId(asset_id, email, extended)                       
            result.append(res_asset)

    if len(result) == 0:
       #abort(404)
       return '', 204   

    resp = jsonify(result)
    resp.set_etag(generate_etag(json.dumps(result).encode()))
    return resp
    #return jsonify(res)



@REQUEST_API.route('/Project/User/<string:tag>/<string:frameAppid>/<string:email>', methods=['GET'])
def GetProjectListByUser(tag,frameAppid,email):
    """Lectura de todos los projects a los que tiene acceso un usuario
    @param tag: the id   
    @param frameAppid: the id   
    @param email: email del user
    @return: 200: a PROJECT_REQUESTS as a flask/response object \
    with application/json mimetype.
    @raise 404: if extmedia not found
    """
    fn = Metodos() 
    res = fn.funcion_GetProjectListByUser(frameAppid, email)   
    if len(res) == 0:
       #abort(404)
       return '', 204           

    resp = jsonify(res)
    resp.set_etag(generate_etag(json.dumps(res).encode()))
    return resp
    #return jsonify(res)

@REQUEST_API.route('/RightFrameApp/FrameApp/User/<string:tag>/<string:frameAppid>/<string:email>', methods=['GET'])
def GetUserRightFramesAppByFrameAppUser(tag,frameAppid,email):
    """Lectura de todos los projects a los que tiene acceso un usuario
    @param tag: the id   
    @param frameAppid: the id   
    @param email: email del user
    @return: 200: a RIGHTFRAMEAPPS_REQUESTS as a flask/response object \
    with application/json mimetype.
    @raise 404: if extmedia not found
    """
    fn = Metodos() 
    res = fn.funcion_GetUserRightFramesAppByFrameAppUser(frameAppid,email)   
    if len(res) == 0:
       #abort(404)
       return '', 204           

    resp = jsonify(res)
    resp.set_etag(generate_etag(json.dumps(res).encode()))
    return resp
    #return jsonify(res)


