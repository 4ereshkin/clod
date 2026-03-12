from application.registration.contracts import (
    StartRegistrationCommand,
    RegistrationScanPayload,
    RegistrationObjectRef,
    RegistrationParams
)
from interfaces.registration.dto import RegistrationStartMessageDTO


def to_registration_start_command(message: RegistrationStartMessageDTO) -> StartRegistrationCommand:
    dataset = {}
    for scan_id, scan_dto in message.dataset.items():
        traj = None
        if scan_dto.trajectory:
            traj = RegistrationObjectRef(s3_key=scan_dto.trajectory.s3_key, etag=scan_dto.trajectory.etag)

        dataset[scan_id] = RegistrationScanPayload(
            point_cloud=RegistrationObjectRef(s3_key=scan_dto.point_cloud.s3_key, etag=scan_dto.point_cloud.etag),
            trajectory=traj
        )

    params = RegistrationParams(
        crop_radius_m=message.params.crop_radius_m,
        global_voxel_m=message.params.global_voxel_m,
        cascade_voxels_m=message.params.cascade_voxels_m,
        cascade_max_corr_multipliers=message.params.cascade_max_corr_multipliers,
        min_fitness=message.params.min_fitness
    )

    return StartRegistrationCommand(
        workflow_id=message.workflow_id,
        scenario=message.scenario,
        message_version=message.version.message_version,
        pipeline_version=message.version.pipeline_version,
        dataset=dataset,
        params=params
    )