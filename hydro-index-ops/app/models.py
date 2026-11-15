"""Pydantic 出入参模型。"""
from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class BatchCreate(BaseModel):
    note: str | None = Field(default=None, max_length=500, description="备注（可选）")


class BatchOut(BaseModel):
    id: int
    state: str
    cancel_requested: bool
    build_pid: int | None
    phase: str | None
    error: str | None
    cleanup_attempts: int
    created_at: datetime
    updated_at: datetime


class ReadingIn(BaseModel):
    station_id: int = Field(gt=0, description="站号")
    observed_at: datetime = Field(description="观测时间")
    water_level: float = Field(description="水位（米）")
    discharge: float | None = Field(default=None, description="流量（可选）")
    quality: str = Field(default="GOOD", description="GOOD/SUSPECT/BAD")


class ReadingOut(ReadingIn):
    id: int
    revision: int | None = Field(
        default=None, description="修订号；无版本写入的旧行为 NULL"
    )
    ingested_at: datetime


class RevisionIn(BaseModel):
    """一笔修订观测：以 (站号, 观测时刻, 修订号) 唯一识别。"""

    station_id: int = Field(gt=0, description="站号")
    observed_at: datetime = Field(description="观测时间")
    revision: int = Field(
        ge=0, description="修订号；同站同时刻下数值越大版本越新"
    )
    water_level: float = Field(description="水位（米）")
    discharge: float | None = Field(default=None, description="流量（可选）")
    quality: str = Field(default="GOOD", description="GOOD/SUSPECT/BAD")


class CurrentObservationOut(BaseModel):
    """当前观测：某站某观测时刻已收到的最高修订。"""

    station_id: int
    observed_at: datetime
    revision: int
    reading_id: int = Field(description="指向 readings 中已提交的原始行")
    water_level: float
    discharge: float | None
    quality: str
    updated_at: datetime


class RevisionWriteOut(BaseModel):
    deduplicated: bool = Field(
        description="同版同内容重试时为 true：未重复入库，返回已存在的记录"
    )
    current_updated: bool = Field(
        description="本次写入是否把当前观测推进到了该修订（旧修订迟到时为 false）"
    )
    reading: ReadingOut
    current: CurrentObservationOut


class LongTxOut(BaseModel):
    pid: int
    seconds: int
    state: str
    message: str
