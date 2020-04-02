"""The Endpoints to manage the BOOK_REQUESTS"""
import uuid
from datetime import datetime, timedelta
from flask import jsonify, abort, request, Blueprint


from custom import Database, Metodos

from validate_email import validate_email
REQUEST_API = Blueprint('request_api', __name__)

def get_blueprint():
    """Return the blueprint for the main app module"""
    return REQUEST_API

@REQUEST_API.route('/', methods=['GET'])
def PaginaInicio():
    """Retorna la lista de assets para un proyecto dado    
    @return: 200: Página de inicio \
    with application/json mimetype.
    @raise 404: if pagina inicio not found
    """
    
    return 'Página de Inicio', 200

@REQUEST_API.route('/Asset/ByProject/<string:tag>/<string:llamadopor>/<string:uti>/<string:project_id>', methods=['GET'])
def GetAssetListByProjectId(tag,llamadopor,uti,project_id):
    """Retorna la lista de assets para un proyecto dado
    @param tag: the id
    @param llamadopor: the id
    @param uti: the id
    @param project_id: the id
    @return: 200: a ASSET_REQUESTS as a flask/response object \
    with application/json mimetype.
    @raise 404: if asset not found
    """
    fn = Metodos() 
    res = fn.funcion_GetAssetListByProjectId(project_id)   
    if len(res) == 0:
       #abort(404)
       return '', 204
    return jsonify(res)


@REQUEST_API.route('/ExtMedia/Asset/<string:tag>/<string:llamadopor>/<string:uti>/<string:asset_id>/<string:txartela>', methods=['GET'])
def GetExtMediaByAssetId(tag,llamadopor,uti,asset_id,txartela):
    """Lectura del extmedia dado el asset
    @param tag: the id
    @param llamadopor: the id
    @param uti: the id
    @param asset_id: the id
    @param txartela: the id
    @return: 200: a EXTMEDIA_REQUESTS as a flask/response object \
    with application/json mimetype.
    @raise 404: if extmedia not found
    """
    fn = Metodos() 
    res = fn.funcion_GetExtMediaByAssetId(asset_id)   
    if len(res) == 0:
       #abort(404)
       return '', 204
    return jsonify(res)


@REQUEST_API.route('/ExtMedia/Asset/full/<string:tag>/<string:llamadopor>/<string:uti>/<string:asset_id>/<string:txartela>', methods=['GET'])
def GetExtMediaFullByAssetId(tag,llamadopor,uti,asset_id,txartela):
    """Lectura de toda la metadata de extmedia dado el asset
    @param tag: the id
    @param llamadopor: the id
    @param uti: the id
    @param asset_id: the id
    @param txartela: the id
    @return: 200: a EXTMEDIA_REQUESTS as a flask/response object \
    with application/json mimetype.
    @raise 404: if extmedia not found
    """
    fn = Metodos() 
    res = fn.funcion_GetExtMediaFullByAssetId(asset_id)   
    if len(res) == 0:
       #abort(404)
       return '', 204
    return jsonify(res)

