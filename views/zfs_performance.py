"""
ZFS Performance Views (consolidated under Observability)

These routes were previously served from a separate Performance page. They
are now mounted under the /zfs/observability prefix as part of the unified
Observability page. The standalone Performance index and Pool Capacity
pages were removed during consolidation; capacity information is shown
on the Pools detail page.
"""
from fastapi import APIRouter, Request, Depends
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from typing import Optional
from datetime import datetime
from config.templates import templates
from services.zfs_performance import ZFSPerformanceService
from auth.dependencies import get_current_user


router = APIRouter(
    prefix="/zfs/observability",
    tags=["zfs-observability"],
    dependencies=[Depends(get_current_user)],
)
performance_service = ZFSPerformanceService()


@router.get("/pool-iostat", response_class=HTMLResponse)
async def pool_iostat_page(
    request: Request,
    pool: Optional[str] = None,
    interval: int = 2,
    verbose: bool = False,
    latency: bool = False,
    queue: bool = False
):
    """Display pool I/O statistics with live updates"""
    try:
        from services.zfs_pool import ZFSPoolService
        pool_service = ZFSPoolService()

        all_pools = pool_service.list_pools()
        pool_names = [p['name'] for p in all_pools]

        iostat_data = performance_service.get_zpool_iostat(
            pool_name=pool,
            interval=interval,
            count=1,
            verbose=verbose,
            latency=latency,
            queue=queue,
        )

        return templates.TemplateResponse(
            "zfs/observability/pool_iostat.jinja",
            {
                "request": request,
                "all_pools": pool_names,
                "selected_pool": pool,
                "interval": interval,
                "verbose": verbose,
                "latency": latency,
                "queue": queue,
                "iostat_data": iostat_data,
                "page_title": "Pool I/O Statistics",
            },
        )
    except Exception as e:
        return templates.TemplateResponse(
            "zfs/observability/pool_iostat.jinja",
            {
                "request": request,
                "all_pools": [],
                "iostat_data": {},
                "error": str(e),
                "page_title": "Pool I/O Statistics",
            },
        )


@router.get("/api/pool-iostat")
async def pool_iostat_api(
    pool: Optional[str] = None,
    interval: int = 1,
    count: int = 1,
    verbose: bool = False,
    latency: bool = False,
    queue: bool = False,
):
    """API endpoint for fetching live pool I/O stats"""
    try:
        iostat_data = performance_service.get_zpool_iostat(
            pool_name=pool,
            interval=interval,
            count=count,
            verbose=verbose,
            latency=latency,
            queue=queue,
        )
        return JSONResponse(content=iostat_data)
    except Exception as e:
        return JSONResponse(
            content={"error": str(e)},
            status_code=500,
        )


@router.get("/pool-iostat-partial", response_class=HTMLResponse)
async def pool_iostat_partial(
    request: Request,
    pool: Optional[str] = None,
    interval: int = 1,
    verbose: bool = False,
    latency: bool = False,
    queue: bool = False,
):
    """HTMX partial endpoint for pool iostat statistics table"""
    try:
        iostat_data = performance_service.get_zpool_iostat(
            pool_name=pool,
            interval=interval,
            count=1,
            verbose=verbose,
            latency=latency,
            queue=queue,
        )

        statistics = iostat_data.get('statistics', [])

        if not statistics:
            return HTMLResponse(content='<p class="text-center text-text-secondary py-4">No statistics available</p>')

        html = '<table class="min-w-full divide-y divide-border-subtle"><thead class="bg-bg-elevated-2"><tr>'
        html += '<th class="px-4 py-3 text-left text-xs font-medium text-text-secondary uppercase">Device</th>'
        html += '<th class="px-4 py-3 text-left text-xs font-medium text-text-secondary uppercase">Alloc</th>'
        html += '<th class="px-4 py-3 text-left text-xs font-medium text-text-secondary uppercase">Free</th>'
        html += '<th class="px-4 py-3 text-left text-xs font-medium text-text-secondary uppercase">Read Ops</th>'
        html += '<th class="px-4 py-3 text-left text-xs font-medium text-text-secondary uppercase">Write Ops</th>'
        html += '<th class="px-4 py-3 text-left text-xs font-medium text-text-secondary uppercase">Read BW</th>'
        html += '<th class="px-4 py-3 text-left text-xs font-medium text-text-secondary uppercase">Write BW</th>'

        if latency:
            html += '<th class="px-4 py-3 text-left text-xs font-medium text-text-secondary uppercase">Read Lat</th>'
            html += '<th class="px-4 py-3 text-left text-xs font-medium text-text-secondary uppercase">Write Lat</th>'

        if queue:
            html += '<th class="px-4 py-3 text-left text-xs font-medium text-text-secondary uppercase">Sync Q</th>'
            html += '<th class="px-4 py-3 text-left text-xs font-medium text-text-secondary uppercase">Async Q</th>'

        html += '</tr></thead><tbody class="bg-bg-elevated divide-y divide-border-subtle">'

        for stat in statistics:
            indent_level = stat.get('indent_level', 0)
            indent_px = indent_level * 20
            device_style = f'style="padding-left: {indent_px + 16}px;"' if indent_px > 0 else ''

            device_prefix = ''
            if indent_level > 0:
                device_prefix = '<span class="text-text-secondary mr-1">└─ </span>'

            html += '<tr class="hover:bg-bg-elevated-2">'
            html += f'<td class="px-4 py-3 text-sm font-medium text-text-primary" {device_style}>{device_prefix}{stat["device"]}</td>'
            html += f'<td class="px-4 py-3 text-sm text-text-secondary">{stat["alloc"]}</td>'
            html += f'<td class="px-4 py-3 text-sm text-text-secondary">{stat["free"]}</td>'
            html += f'<td class="px-4 py-3 text-sm text-text-primary">{stat["read_ops"]}</td>'
            html += f'<td class="px-4 py-3 text-sm text-text-primary">{stat["write_ops"]}</td>'
            html += f'<td class="px-4 py-3 text-sm text-primary-400">{stat["read_bw"]}</td>'
            html += f'<td class="px-4 py-3 text-sm text-success-400">{stat["write_bw"]}</td>'

            if latency:
                html += f'<td class="px-4 py-3 text-sm text-text-secondary">{stat.get("read_latency", "-")}</td>'
                html += f'<td class="px-4 py-3 text-sm text-text-secondary">{stat.get("write_latency", "-")}</td>'

            if queue:
                html += f'<td class="px-4 py-3 text-sm text-text-secondary">{stat.get("sync_queue", "-")}</td>'
                html += f'<td class="px-4 py-3 text-sm text-text-secondary">{stat.get("async_queue", "-")}</td>'

            html += '</tr>'

        html += '</tbody></table>'
        return HTMLResponse(content=html)

    except Exception as e:
        return HTMLResponse(content=f'<p class="text-center text-danger-400 py-4">Error loading data: {str(e)}</p>')


@router.get("/pool-iostat-raw", response_class=HTMLResponse)
async def pool_iostat_raw(
    pool: Optional[str] = None,
    interval: int = 1,
    verbose: bool = False,
    latency: bool = False,
    queue: bool = False,
):
    """HTMX partial endpoint for pool iostat raw output"""
    try:
        iostat_data = performance_service.get_zpool_iostat(
            pool_name=pool,
            interval=interval,
            count=1,
            verbose=verbose,
            latency=latency,
            queue=queue,
        )

        raw_output = iostat_data.get('raw_output', 'No data')
        return HTMLResponse(content=raw_output)

    except Exception as e:
        return HTMLResponse(content=f'Error: {str(e)}')


@router.get("/arc-realtime", response_class=HTMLResponse)
async def arc_realtime_page(
    request: Request,
    interval: int = 2,
):
    """Display real-time ARC statistics with raw output"""
    try:
        raw_arcstats = performance_service.get_raw_arcstats()
        system = performance_service.system

        return templates.TemplateResponse(
            "zfs/observability/arc_realtime.jinja",
            {
                "request": request,
                "raw_arcstats": raw_arcstats,
                "system": system,
                "interval": interval,
                "page_title": "ARC Statistics",
            },
        )
    except Exception as e:
        return templates.TemplateResponse(
            "zfs/observability/arc_realtime.jinja",
            {
                "request": request,
                "raw_arcstats": {},
                "system": performance_service.system,
                "error": str(e),
                "page_title": "ARC Statistics",
            },
        )


@router.get("/api/arc-stats")
async def arc_stats_api():
    """API endpoint for fetching live ARC stats (parsed)"""
    try:
        arc_stats = performance_service._read_arc_stats()
        arc_stats['timestamp'] = datetime.now().isoformat()
        return JSONResponse(content=arc_stats)
    except Exception as e:
        return JSONResponse(
            content={"error": str(e)},
            status_code=500,
        )


@router.get("/arc-stats-raw", response_class=HTMLResponse)
async def arc_stats_raw():
    """HTMX partial endpoint for raw ARC stats output"""
    try:
        raw_arcstats = performance_service.get_raw_arcstats()

        if raw_arcstats.get('error'):
            error_msg = raw_arcstats['error']
            if raw_arcstats.get('system') == 'FreeBSD':
                error_msg += '\n\nNote: Install zfs-stats with: pkg install zfs-stats'
            return HTMLResponse(content=f'<span class="text-warning-400">{error_msg}</span>')

        return HTMLResponse(content=raw_arcstats.get('output', 'No data available'))

    except Exception as e:
        return HTMLResponse(content=f'<span class="text-danger-400">Error: {str(e)}</span>')


@router.get("/processes", response_class=HTMLResponse)
async def zfs_processes_page(request: Request, interval: int = 5):
    """Display ZFS processes with live updates"""
    try:
        processes = performance_service.get_zfs_processes(sort_by_cpu=True)

        return templates.TemplateResponse(
            "zfs/observability/processes.jinja",
            {
                "request": request,
                "processes": processes,
                "interval": interval,
                "page_title": "ZFS Processes",
            },
        )
    except Exception as e:
        return templates.TemplateResponse(
            "zfs/observability/processes.jinja",
            {
                "request": request,
                "processes": [],
                "interval": interval,
                "error": str(e),
                "page_title": "ZFS Processes",
            },
        )


@router.get("/api/processes")
async def processes_api():
    """API endpoint for fetching live process data - sorted by CPU%"""
    try:
        processes = performance_service.get_zfs_processes(sort_by_cpu=True)
        return JSONResponse(content={"processes": processes})
    except Exception as e:
        return JSONResponse(
            content={"error": str(e)},
            status_code=500,
        )


@router.get("/processes-summary", response_class=HTMLResponse)
async def processes_summary():
    """HTMX partial endpoint for process summary cards"""
    try:
        processes = performance_service.get_zfs_processes()
        running_count = sum(1 for p in processes if p.get('status') == 'running')
        sleeping_count = sum(1 for p in processes if p.get('status') == 'sleeping')

        html = f'''
        <div class="bg-bg-elevated rounded-lg shadow p-6">
            <div class="text-sm text-text-secondary mb-1">Total ZFS Processes</div>
            <div class="text-2xl font-bold text-text-primary">{len(processes)}</div>
        </div>

        <div class="bg-bg-elevated rounded-lg shadow p-6">
            <div class="text-sm text-text-secondary mb-1">Running Processes</div>
            <div class="text-2xl font-bold text-success-400">{running_count}</div>
        </div>

        <div class="bg-bg-elevated rounded-lg shadow p-6">
            <div class="text-sm text-text-secondary mb-1">Sleeping Processes</div>
            <div class="text-2xl font-bold text-primary-400">{sleeping_count}</div>
        </div>
        '''
        return HTMLResponse(content=html)
    except Exception as e:
        return HTMLResponse(content=f'<p class="text-danger-400">Error: {str(e)}</p>')


@router.get("/processes/download")
async def download_processes():
    """Download endpoint for ZFS processes list"""
    try:
        processes = performance_service.get_zfs_processes(sort_by_cpu=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        output_lines = []
        output_lines.append("=" * 80)
        output_lines.append("ZFS Process List")
        output_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        output_lines.append("=" * 80)
        output_lines.append("")

        if processes:
            output_lines.append(f"{'PID':<10} {'Process Name':<30} {'User':<15} {'CPU %':<10} {'Mem %':<10} {'Status':<10}")
            output_lines.append("-" * 90)

            for proc in processes:
                output_lines.append(
                    f"{proc['pid']:<10} "
                    f"{proc['name']:<30} "
                    f"{proc['username']:<15} "
                    f"{proc['cpu_percent']:<10.1f} "
                    f"{proc['memory_percent']:<10.1f} "
                    f"{proc['status']:<10}"
                )

            output_lines.append("")
            output_lines.append(f"Total processes: {len(processes)}")
        else:
            output_lines.append("No ZFS processes currently running")

        output_lines.append("")
        output_lines.append("=" * 80)

        content = "\n".join(output_lines)

        return PlainTextResponse(
            content=content,
            headers={
                "Content-Disposition": f'attachment; filename="zfs_processes_{timestamp}.txt"',
            },
        )
    except Exception as e:
        return PlainTextResponse(
            content=f"Error generating process list: {str(e)}",
            status_code=500,
        )


@router.get("/processes-table", response_class=HTMLResponse)
async def processes_table():
    """HTMX partial endpoint for processes table - sorted by CPU%"""
    try:
        processes = performance_service.get_zfs_processes(sort_by_cpu=True)

        if not processes:
            return HTMLResponse(content='<p class="text-text-secondary text-center py-8">No ZFS processes currently running</p>')

        html = '<table class="min-w-full divide-y divide-border-subtle">'
        html += '''<thead class="bg-bg-elevated-2">
            <tr>
                <th class="px-4 py-3 text-left text-xs font-medium text-text-secondary uppercase tracking-wider">PID</th>
                <th class="px-4 py-3 text-left text-xs font-medium text-text-secondary uppercase tracking-wider">Process Name</th>
                <th class="px-4 py-3 text-left text-xs font-medium text-text-secondary uppercase tracking-wider">User</th>
                <th class="px-4 py-3 text-left text-xs font-medium text-text-secondary uppercase tracking-wider">CPU %</th>
                <th class="px-4 py-3 text-left text-xs font-medium text-text-secondary uppercase tracking-wider">Memory %</th>
                <th class="px-4 py-3 text-left text-xs font-medium text-text-secondary uppercase tracking-wider">Status</th>
            </tr>
        </thead>'''
        html += '<tbody class="bg-bg-elevated divide-y divide-border-subtle">'

        for proc in processes:
            status = proc.get('status', 'unknown')
            if status == 'running':
                badge_class = 'badge-success'
            elif status == 'sleeping':
                badge_class = 'badge-info'
            else:
                badge_class = 'bg-bg-elevated-2 text-text-primary'

            html += f'''<tr class="hover:bg-bg-elevated-2">
                <td class="px-4 py-3 text-sm text-text-primary whitespace-nowrap">{proc['pid']}</td>
                <td class="px-4 py-3 text-sm font-medium text-text-primary">{proc['name']}</td>
                <td class="px-4 py-3 text-sm text-text-secondary">{proc['username']}</td>
                <td class="px-4 py-3 text-sm text-text-primary">{proc['cpu_percent']:.1f}%</td>
                <td class="px-4 py-3 text-sm text-text-primary">{proc['memory_percent']:.1f}%</td>
                <td class="px-4 py-3 text-sm">
                    <span class="px-2 py-1 text-xs rounded-full {badge_class}">{status}</span>
                </td>
            </tr>'''

        html += '</tbody></table>'
        return HTMLResponse(content=html)
    except Exception as e:
        return HTMLResponse(content=f'<p class="text-center text-danger-400 py-4">Error loading processes: {str(e)}</p>')


@router.get("/dataset-space", response_class=HTMLResponse)
async def dataset_space_page(
    request: Request,
    dataset: Optional[str] = None,
    recursive: bool = False,
):
    """Display dataset space usage"""
    try:
        from services.zfs_dataset import ZFSDatasetService
        dataset_service = ZFSDatasetService()

        all_datasets = dataset_service.list_datasets()
        dataset_names = [d['name'] for d in all_datasets]

        space_usage = performance_service.get_dataset_space_usage(
            dataset_name=dataset,
            recursive=recursive,
        )

        return templates.TemplateResponse(
            "zfs/observability/dataset_space.jinja",
            {
                "request": request,
                "all_datasets": dataset_names,
                "selected_dataset": dataset,
                "recursive": recursive,
                "space_usage": space_usage,
                "page_title": "Dataset Space Usage",
            },
        )
    except Exception as e:
        return templates.TemplateResponse(
            "zfs/observability/dataset_space.jinja",
            {
                "request": request,
                "all_datasets": [],
                "space_usage": [],
                "error": str(e),
                "page_title": "Dataset Space Usage",
            },
        )


@router.get("/api/dataset-space")
async def dataset_space_api(
    dataset: Optional[str] = None,
    recursive: bool = False,
):
    """API endpoint for fetching dataset space usage"""
    try:
        space_usage = performance_service.get_dataset_space_usage(
            dataset_name=dataset,
            recursive=recursive,
        )
        return JSONResponse(content={"datasets": space_usage})
    except Exception as e:
        return JSONResponse(
            content={"error": str(e)},
            status_code=500,
        )


@router.get("/vdev-stats", response_class=HTMLResponse)
async def vdev_stats_page(
    request: Request,
    pool: Optional[str] = None,
):
    """Display per-vdev statistics"""
    try:
        from services.zfs_pool import ZFSPoolService
        pool_service = ZFSPoolService()

        all_pools = pool_service.list_pools()
        pool_names = [p['name'] for p in all_pools]

        vdev_stats = []
        selected_pool = pool or (pool_names[0] if pool_names else None)

        if selected_pool:
            vdev_stats = performance_service.get_vdev_stats(selected_pool)

        return templates.TemplateResponse(
            "zfs/observability/vdev_stats.jinja",
            {
                "request": request,
                "all_pools": pool_names,
                "selected_pool": selected_pool,
                "vdev_stats": vdev_stats,
                "page_title": "Per-VDEV Statistics",
            },
        )
    except Exception as e:
        return templates.TemplateResponse(
            "zfs/observability/vdev_stats.jinja",
            {
                "request": request,
                "all_pools": [],
                "vdev_stats": [],
                "error": str(e),
                "page_title": "Per-VDEV Statistics",
            },
        )


@router.get("/api/vdev-stats")
async def vdev_stats_api(pool: str):
    """API endpoint for fetching vdev stats"""
    try:
        vdev_stats = performance_service.get_vdev_stats(pool)
        return JSONResponse(content={"vdevs": vdev_stats})
    except Exception as e:
        return JSONResponse(
            content={"error": str(e)},
            status_code=500,
        )


@router.get("/vdev-stats-table", response_class=HTMLResponse)
async def vdev_stats_table(pool: str):
    """HTMX partial endpoint for vdev stats table"""
    try:
        vdev_stats = performance_service.get_vdev_stats(pool)

        if not vdev_stats:
            return HTMLResponse(content='<p class="text-text-secondary text-center py-8">No VDEV statistics available for this pool</p>')

        html = '<table class="min-w-full">'
        html += '''<thead class="bg-bg-elevated-2 text-text-primary">
            <tr>
                <th class="px-4 py-3 text-left text-sm font-semibold">Device / VDEV</th>
                <th class="px-4 py-3 text-right text-sm font-semibold">Allocated</th>
                <th class="px-4 py-3 text-right text-sm font-semibold">Free</th>
                <th class="px-4 py-3 text-right text-sm font-semibold">Read Ops</th>
                <th class="px-4 py-3 text-right text-sm font-semibold">Write Ops</th>
                <th class="px-4 py-3 text-right text-sm font-semibold">Read BW</th>
                <th class="px-4 py-3 text-right text-sm font-semibold">Write BW</th>
            </tr>
        </thead>'''
        html += '<tbody class="bg-bg-elevated font-mono text-sm">'

        for vdev in vdev_stats:
            name = vdev.get('name', '')
            is_raidz = 'raidz' in name or 'mirror' in name
            is_disk = '-' in name or 'nvme' in name or 'sd' in name or 'loop' in name

            row_class = ''
            if is_raidz:
                row_class = 'bg-primary-900/30 border-t-2 border-primary-500/50'
            elif is_disk:
                row_class = 'border-l-4 border-primary-500/30'

            name_class = ''
            if is_raidz:
                name_class = 'font-bold text-primary-300'
            elif is_disk:
                name_class = 'pl-12 text-text-secondary'
            else:
                name_class = 'font-semibold'

            read_ops = vdev.get('read_ops', '0')
            write_ops = vdev.get('write_ops', '0')
            read_bw = vdev.get('read_bw', '-')
            write_bw = vdev.get('write_bw', '-')

            read_ops_class = 'text-primary-400 font-semibold' if int(read_ops) > 0 else 'text-text-secondary'
            write_ops_class = 'text-success-400 font-semibold' if int(write_ops) > 0 else 'text-text-secondary'
            read_bw_class = 'text-primary-400 font-bold' if (read_bw != '0' and read_bw != '-') else 'text-text-secondary'
            write_bw_class = 'text-success-400 font-bold' if (write_bw != '0' and write_bw != '-') else 'text-text-secondary'

            html += f'''<tr class="{row_class} hover:bg-bg-elevated-2">
                <td class="px-4 py-2 {name_class}">{name}</td>
                <td class="px-4 py-2 text-right text-text-secondary">{vdev.get('alloc', '-')}</td>
                <td class="px-4 py-2 text-right text-text-secondary">{vdev.get('free', '-')}</td>
                <td class="px-4 py-2 text-right {read_ops_class}">{read_ops}</td>
                <td class="px-4 py-2 text-right {write_ops_class}">{write_ops}</td>
                <td class="px-4 py-2 text-right {read_bw_class}">{read_bw}</td>
                <td class="px-4 py-2 text-right {write_bw_class}">{write_bw}</td>
            </tr>'''

        html += '</tbody></table>'
        return HTMLResponse(content=html)
    except Exception as e:
        return HTMLResponse(content=f'<p class="text-center text-danger-400 py-4">Error loading VDEV stats: {str(e)}</p>')


@router.get("/system-iostat", response_class=HTMLResponse)
async def system_iostat_page(
    request: Request,
    interval: int = 2,
    extended: bool = True,
):
    """Display system I/O statistics (Linux iostat / FreeBSD gstat)"""
    try:
        system = performance_service.system

        if system == 'Linux':
            iostat_data = performance_service.get_system_iostat(
                extended=extended,
                interval=interval,
                count=1,
            )
        elif system == 'FreeBSD':
            iostat_data = performance_service.get_gstat(
                interval=interval,
                count=1,
            )
        else:
            iostat_data = {'error': f'System I/O stats not available on {system}'}

        return templates.TemplateResponse(
            "zfs/observability/system_iostat.jinja",
            {
                "request": request,
                "system": system,
                "interval": interval,
                "extended": extended,
                "iostat_data": iostat_data,
                "page_title": "System I/O Statistics",
            },
        )
    except Exception as e:
        return templates.TemplateResponse(
            "zfs/observability/system_iostat.jinja",
            {
                "request": request,
                "system": performance_service.system,
                "iostat_data": {},
                "error": str(e),
                "page_title": "System I/O Statistics",
            },
        )


@router.get("/api/system-iostat")
async def system_iostat_api(
    interval: int = 1,
    count: int = 1,
    extended: bool = True,
):
    """API endpoint for fetching system I/O stats"""
    try:
        system = performance_service.system

        if system == 'Linux':
            iostat_data = performance_service.get_system_iostat(
                extended=extended,
                interval=interval,
                count=count,
            )
        elif system == 'FreeBSD':
            iostat_data = performance_service.get_gstat(
                interval=interval,
                count=count,
            )
        else:
            iostat_data = {'error': f'System I/O stats not available on {system}'}

        return JSONResponse(content=iostat_data)
    except Exception as e:
        return JSONResponse(
            content={"error": str(e)},
            status_code=500,
        )


@router.get("/system-iostat-output", response_class=HTMLResponse)
async def system_iostat_output(
    interval: int = 1,
    extended: bool = True,
):
    """HTMX partial endpoint for system iostat output"""
    try:
        system = performance_service.system

        if system == 'Linux':
            iostat_data = performance_service.get_system_iostat(
                extended=extended,
                interval=interval,
                count=1,
            )
        elif system == 'FreeBSD':
            iostat_data = performance_service.get_gstat(
                interval=interval,
                count=1,
            )
        else:
            return HTMLResponse(content=f'<span class="text-warning-400">System I/O stats not available on {system}</span>')

        if iostat_data.get('error'):
            error_msg = iostat_data['error']
            if system == 'Linux':
                error_msg += '\n\nNote: Install the sysstat package to enable iostat:\n  sudo apt install sysstat  (Debian/Ubuntu)\n  sudo pacman -S sysstat    (Arch Linux)'
            return HTMLResponse(content=f'<span class="text-warning-400">{error_msg}</span>')

        return HTMLResponse(content=iostat_data.get('output', 'No data available'))

    except Exception as e:
        return HTMLResponse(content=f'<span class="text-danger-400">Error: {str(e)}</span>')
