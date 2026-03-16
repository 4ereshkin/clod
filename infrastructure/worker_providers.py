from dishka import Provider, Scope, provide

from application.common.config import AppSettings
from infrastructure.s3 import S3Client
from point_cloud.activities.ingest_activities_v1 import IngestActivitiesV1
from point_cloud.activities.registration_activities_v1 import RegistrationActivitiesV1


class WorkerProvider(Provider):
    ingest_activities_v1 = provide(IngestActivitiesV1, scope=Scope.APP)
    registration_activities_v1 = provide(RegistrationActivitiesV1, scope=Scope.APP)

    @provide(scope=Scope.APP)
    def get_s3_client(self, config: AppSettings) -> S3Client:
        return S3Client(config.s3)
