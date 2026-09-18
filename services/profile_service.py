from typing import Optional

class ProfileService:
    def __init__(self, db_store, user_service):   # user_service – экземпляр UserService
        self.db_store = db_store
        self.user_service = user_service

    async def set_username(self, email: str, username: Optional[str]) -> bool:
        return await self.user_service.set_username(email, username)

    async def set_first_name(self, email: str, name: str) -> bool:
        return await self.user_service.set_first_name(email, name)

    async def set_bio(self, email: str, bio: str) -> bool:
        return await self.user_service.set_bio(email, bio)

    async def set_show_email(self, email: str, show: bool) -> bool:
        return await self.user_service.set_show_email(email, show)

    async def get_user_info(self, target_email: str, requester_email: Optional[str] = None) -> Optional[dict]:
        return await self.user_service.get_user_info(target_email, requester_email)
