import redis


class TokenRepository:
    def __init__(self, redis_host: str, redis_port: str) -> None:
        self.r_db = redis.Redis(
            host=redis_host, port=redis_port, db=0, decode_responses=True
        )

    def remove_all_user_tokens(self, user_id: str):
        keys_to_delete = self.r_db.keys(f"auth:{user_id}:*")
        for key in keys_to_delete:
            self.r_db.delete(key)
