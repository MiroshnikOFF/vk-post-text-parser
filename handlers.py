from multiprocessing import Process

from manager import start_service


class RequestHandler:
    """

    """
    @staticmethod
    def start():
        """

        """
        process = Process(target=start_service)
        process.daemon = True
        process.start()

