class AppState:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(AppState, cls).__new__(cls)
            cls.websocket = None
            cls.mgindb_url = 'https://mgindb.com'
            cls.version = '0.1.0'
            cls.license = None
            cls.auth_data = {}
            cls.scheduler_task = None
            cls.scheduled_tasks = {}
            cls.sessions = {}
            cls.config_store = {}
            cls.data_store = {}
            cls.expires_store = {}
            cls.indices = {}
            cls.monitor_subscribers = set()
            cls.sub_pub = {}

            # Changes tracking
            cls.data_has_changed = False
            cls.indices_has_changed = False
        return cls._instance
