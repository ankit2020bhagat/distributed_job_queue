

from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    AsyncSession,
    async_sessionmaker
)
from sqlalchemy.pool import NullPool, QueuePool

from app.core.config import settings
from app.models.job import Base


class DatabaseManager:


    def __init__(self):
        """Initialize database engine and session factory"""
        self._engine = None
        self._session_factory = None

    def init_engine(self):

        if self._engine is None:
            self._engine = create_async_engine(
                settings.database.url,
                echo=settings.debug,
                pool_size=settings.database.pool_size,
                max_overflow=settings.database.max_overflow,
                pool_pre_ping=True,  # Test connections before using
                pool_recycle=3600,  # Recycle connections after 1 hour
                poolclass=QueuePool,
            )

            self._session_factory = async_sessionmaker(
                self._engine,
                class_=AsyncSession,
                expire_on_commit=False,
                autocommit=False,
                autoflush=False,
            )

    @property
    def engine(self):
        """Get database engine, initializing if needed"""
        if self._engine is None:
            self.init_engine()
        return self._engine

    @property
    def session_factory(self):
        """Get session factory, initializing if needed"""
        if self._session_factory is None:
            self.init_engine()
        return self._session_factory

    async def create_tables(self):

        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def drop_tables(self):

        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)

    async def close(self):
        """Close database engine and cleanup connections"""
        if self._engine is not None:
            await self._engine.dispose()
            self._engine = None
            self._session_factory = None

    async def health_check(self) -> bool:

        try:
            async with self.session_factory() as session:
                await session.execute("SELECT 1")
                return True
        except Exception as e:
            print(f"Database health check failed: {e}")
            return False


# Global database manager instance
db_manager = DatabaseManager()


async def get_db() -> AsyncGenerator[AsyncSession, None]:

    async with db_manager.session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def init_db():

    db_manager.init_engine()
    await db_manager.create_tables()
    print("✓ Database initialized successfully")


async def close_db():
    
    await db_manager.close()
    print("✓ Database connections closed")