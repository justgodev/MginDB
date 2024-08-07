class AppState:
    _instance = None  # Class-level attribute to hold the singleton instance

    def __new__(cls):
        """
        Ensures that only one instance of AppState is created (singleton pattern).

        This method checks if an instance of AppState already exists. If not, it creates a new instance,
        initializes various attributes related to the application state, and assigns it to the class-level
        attribute _instance. If an instance already exists, it returns the existing instance.
        """
        if cls._instance is None:
            # Create a new instance if one does not already exist
            cls._instance = super(AppState, cls).__new__(cls)
            # Initialize attributes
            cls.websocket = None  # WebSocket connection placeholder
            cls.mgindb_url = 'https://mgindb.com'  # URL for MginDB
            cls.version = '0.1.5'  # Version of the application
            cls.license = None  # License information
            cls.auth_data = {}  # Authentication data
            cls.scheduler_task = None  # Current scheduler task
            cls.scheduled_tasks = {}  # Dictionary of scheduled tasks
            cls.sessions = {}  # Active sessions
            cls.config_store = {}  # Configuration store
            cls.blockchain = []  #  Blockchain
            cls.blockchain_pending_transactions = []  # Pending transactions
            cls.blockchain_mempool = []  # Mempool transactions
            cls.wallets = {}  # Wallets
            cls.data_store = {}  # Data store
            cls.expires_store = {}  # Expiry times for data entries
            cls.indices = {}  # Indices for data
            cls.monitor_subscribers = set()  # Set of monitor subscribers
            cls.node_subscribers = set()  # Set of node subscribers
            cls.node_lite_subscribers = set()  # Set of node lite subscribers
            cls.sub_pub = {}  # Publish/subscribe dictionary

            # Cache
            cls.data_store_cache = {}
            cls.data_store_cache_keys_expiration = {}
            cls.data_store_key_command_mapping = {}

            # Blockchain requests
            cls.blockchain_requests = {}
            cls.blockchain_blocks_requests = {}
            cls.blockchain_txns_requests = {}

            # Changes tracking
            cls.data_has_changed = False  # Flag to track data changes
            cls.indices_has_changed = False  # Flag to track indices changes
            cls.blockchain_has_changed = False  # Flag to track blockchain changes
            cls.blockchain_pending_transactions_has_changed = False  # Flag to track blockchain pending transactions changes
            cls.blockchain_wallets_has_changed = False  # Flag to track blockchain wallets changes
        return cls._instance  # Return the singleton instance
