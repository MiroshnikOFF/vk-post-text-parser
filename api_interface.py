import flask
import waitress

from logger import APILogger
from handlers import RequestHandler

logger = APILogger()
handler = RequestHandler()

app = flask.Flask(__name__)


@app.route('/health', methods=['GET'])
def health():
    """
    Возвращает статус "UP" и GUID запуска сервиса
    """
    logger.info(f'API: GET_health')
    result = {
        "status": "UP"
    }
    return flask.jsonify(result)


@app.route('/heartbeat', methods=['GET'])
def heartbeat():
    """
    Возвращает Ok, если сервис функционирующий
    """
    logger.info(f'API: GET_heartbeat')
    return 'Ok'


@app.route('/status', methods=['GET'])
def status():
    """
    Возвращает статус сервиса
    """
    logger.info(f'API: GET_status')
    return flask.Response(status=200)


@app.route('/start', methods=['POST'])
def calculate():
    """
    Отправляет запрос на старт
    """
    logger.info(f'API: POST_start')
    handler.start()
    result = {
        "status": "UP"
    }
    return flask.jsonify(result)


# @app.route('/stop', methods=['POST'])
# def stop():
#     """
#     Отправка запроса на остановку расчёта
#     """
#     logger.info(f'API: POST_stop')
#     supervisor.stop(flask.request.json)
#     return flask.Response(status=200)
#
#
# @app.route('/status/<_config>/<_id>', methods=['GET'])
# def get_calculation_status(_config, _id):
#     """
#     Возвращает текущие шаги расчёта
#     """
#     logger.info(f'API: GET_status; Config: {_config}; Identifier: {_id}')
#     _status = 200
#     data = supervisor.get_calculation_status(_config, _id)
#     result = {
#         'result': 1,
#         'data': data
#     }
#     if data is None:
#         _status = 404
#         result = {
#             'result': 0,
#             'data': dict()
#         }
#     return flask.jsonify(result), _status


if __name__ == '__main__':
    print('startup')
    waitress.serve(
        app=app,
        host='0.0.0.0',
        port=18100
    )
