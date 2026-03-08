from fastapi import APIRouter

import views.auth
import views.dashboard
import views.utils_shell
import views.utils_text
import views.utils_files
import views.utils_scrub
import views.utils_logs
import views.zfs_pools
import views.zfs_datasets
import views.zfs_snapshots
import views.zfs_replication
import views.zfs_observability
import views.zfs_performance
import views.utils_smart
import views.utils_ssh
import views.utils_services
import views.utils_settings
import views.fleet


router = APIRouter()

# ZFS Management Routes
router.include_router(views.zfs_pools.router)
router.include_router(views.zfs_datasets.router)
router.include_router(views.zfs_snapshots.router)
router.include_router(views.zfs_replication.router)
router.include_router(views.zfs_observability.router)
router.include_router(views.zfs_performance.router)

# Fleet Monitoring Routes
router.include_router(views.fleet.router)

# Utilities Routes (organized under /utils)
router.include_router(views.utils_scrub.router, prefix="/utils")
router.include_router(views.utils_shell.router, prefix="/utils/shell")
router.include_router(views.utils_text.router, prefix="/utils/text")
router.include_router(views.utils_files.router, prefix="/utils/files")
router.include_router(views.utils_smart.router, prefix="/utils/smart")
router.include_router(views.utils_ssh.router, prefix="/utils/ssh")
router.include_router(views.utils_logs.router, prefix="/utils/logs")
router.include_router(views.utils_services.router, prefix="/utils/services")
router.include_router(views.utils_settings.router, prefix="/utils/settings")

# Authentication and Dashboard
router.include_router(views.auth.router, prefix="/login")
router.include_router(views.dashboard.router)
