from sqlalchemy import Column, DateTime, Numeric, String, func
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Event(Base):
    """Trip event persisted from Kafka."""
    __tablename__ = "events"

    id = Column(String(50), primary_key=True)
    vehicle_id = Column(String(100), nullable=False)
    driver_id = Column(String(100), nullable=False)
    user_id = Column(String(100), nullable=False)
    duration = Column(String(50), nullable=False)
    distance_km = Column(Numeric(10, 2), nullable=False)
    amount = Column(Numeric(10, 2), nullable=False)
    created_at = Column(DateTime, server_default=func.now())

    def __repr__(self):
        return (
            f"<Event(id={self.id}, vehicle_id='{self.vehicle_id}', "
            f"driver_id='{self.driver_id}', user_id='{self.user_id}', "
            f"amount={self.amount})>"
        )
