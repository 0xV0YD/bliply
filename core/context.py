from data.providers import load_providers

class Context:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Context, cls).__new__(cls)
            cls._instance.providers = load_providers()
            cls._instance.provider_dict = {p.name.lower(): p for p in cls._instance.providers}
            cls._instance.method_workers = {}
        return cls._instance

    @classmethod
    def get_providers(cls):
        return cls().providers

    @classmethod
    def get_provider_dict(cls):
        return cls().provider_dict

    @classmethod
    def get_method_workers(cls):
        return cls().method_workers
