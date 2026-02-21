"""
ZFS Observability Views
Provides web interface for viewing ZFS logs, events, and history
"""
from fastapi import APIRouter, Request, Query, Form, Depends
from fastapi.responses import HTMLResponse, RedirectResponse, PlainTextResponse, StreamingResponse
from typing import Annotated, Optional
from datetime import datetime
import io
import zipfile
from config.templates import templates
from services.zfs_observability import ZFSObservabilityService
from auth.dependencies import get_current_user


router = APIRouter(prefix="/zfs/observability", tags=["zfs-observability"], dependencies=[Depends(get_current_user)])
observability_service = ZFSObservabilityService()


@router.get("/", response_class=HTMLResponse)
async def observability_index(request: Request):
    """Main observability dashboard with overview of all log sources"""
    try:
        # Only fetch ARC summary which is fast (reads from /proc)
        # Pool history and events can be very slow on systems with lots of data,
        # so we load those via separate page visits or HTMX partials
        arc_summary = observability_service.get_arc_summary()
        
        return templates.TemplateResponse(
            "zfs/observability/index.jinja",
            {
                "request": request,
                "recent_history": [],  # Loaded via HTMX partial for performance
                "recent_events": [],   # Loaded via HTMX partial for performance
                "arc_summary": arc_summary,
                "page_title": "ZFS Observability"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "zfs/observability/index.jinja",
            {
                "request": request,
                "recent_history": [],
                "recent_events": [],
                "arc_summary": {},
                "error": str(e),
                "page_title": "ZFS Observability"
            }
        )


@router.get("/recent-history-partial", response_class=HTMLResponse)
async def recent_history_partial(request: Request):
    """HTMX partial for loading recent pool history asynchronously"""
    try:
        # Get first available pool to show recent history
        from services.zfs_pool import ZFSPoolService
        pool_service = ZFSPoolService()
        pools = pool_service.list_pools()
        
        recent_history = []
        if pools:
            # Get history from the first pool only for the preview
            first_pool = pools[0]['name']
            recent_history = observability_service.get_pool_history(
                pool_name=first_pool,
                limit=10
            )
        
        # Render just the table body
        if recent_history:
            html = '<table class="min-w-full divide-y divide-border-subtle"><thead class="bg-bg-elevated-2"><tr>'
            html += '<th class="px-4 py-3 text-left text-xs font-medium text-text-secondary uppercase tracking-wider">Time</th>'
            html += '<th class="px-4 py-3 text-left text-xs font-medium text-text-secondary uppercase tracking-wider">Command</th>'
            html += '<th class="px-4 py-3 text-left text-xs font-medium text-text-secondary uppercase tracking-wider">User</th>'
            html += '<th class="px-4 py-3 text-left text-xs font-medium text-text-secondary uppercase tracking-wider">Host</th>'
            html += '</tr></thead><tbody class="bg-bg-elevated divide-y divide-border-subtle">'
            
            for entry in recent_history:
                html += f'''<tr class="hover:bg-bg-elevated-2 transition-colors">
                    <td class="px-4 py-3 text-sm text-text-primary whitespace-nowrap font-mono">{entry.get('timestamp', '')}</td>
                    <td class="px-4 py-3 text-sm text-text-primary font-mono">{entry.get('command', '')}</td>
                    <td class="px-4 py-3 text-sm text-text-secondary">{entry.get('user') or '-'}</td>
                    <td class="px-4 py-3 text-sm text-text-secondary">{entry.get('host') or '-'}</td>
                </tr>'''
            
            html += '</tbody></table>'
            return HTMLResponse(content=html)
        else:
            return HTMLResponse(content='''
                <div class="text-center py-8">
                    <svg class="w-16 h-16 mx-auto text-text-tertiary mb-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"/>
                    </svg>
                    <p class="text-text-secondary">No pool history available</p>
                </div>
            ''')
    except Exception as e:
        return HTMLResponse(content=f'<p class="text-danger-400 py-4">Error loading history: {str(e)}</p>')


@router.get("/recent-events-partial", response_class=HTMLResponse)
async def recent_events_partial(request: Request):
    """HTMX partial for loading recent pool events asynchronously"""
    try:
        # Get first available pool to show recent events
        from services.zfs_pool import ZFSPoolService
        pool_service = ZFSPoolService()
        pools = pool_service.list_pools()
        
        recent_events = []
        if pools:
            # Get events from the first pool only for the preview
            first_pool = pools[0]['name']
            all_events = observability_service.get_pool_events(pool_name=first_pool)
            recent_events = all_events[-10:] if all_events else []
        
        # Render just the table body
        if recent_events:
            html = '<table class="min-w-full divide-y divide-border-subtle"><thead class="bg-bg-elevated-2"><tr>'
            html += '<th class="px-4 py-3 text-left text-xs font-medium text-text-secondary uppercase tracking-wider">Time</th>'
            html += '<th class="px-4 py-3 text-left text-xs font-medium text-text-secondary uppercase tracking-wider">Class</th>'
            html += '<th class="px-4 py-3 text-left text-xs font-medium text-text-secondary uppercase tracking-wider">Pool</th>'
            html += '<th class="px-4 py-3 text-left text-xs font-medium text-text-secondary uppercase tracking-wider">Details</th>'
            html += '</tr></thead><tbody class="bg-bg-elevated divide-y divide-border-subtle">'
            
            for event in recent_events:
                html += f'''<tr class="hover:bg-bg-elevated-2 transition-colors">
                    <td class="px-4 py-3 text-sm text-text-primary whitespace-nowrap font-mono">{event.get('time', '')}</td>
                    <td class="px-4 py-3 text-sm"><span class="badge badge-info">{event.get('class', '')}</span></td>
                    <td class="px-4 py-3 text-sm text-text-primary">{event.get('pool') or '-'}</td>
                    <td class="px-4 py-3 text-sm text-text-secondary">{event.get('details') or '-'}</td>
                </tr>'''
            
            html += '</tbody></table>'
            return HTMLResponse(content=html)
        else:
            return HTMLResponse(content='''
                <div class="text-center py-8">
                    <svg class="w-16 h-16 mx-auto text-text-tertiary mb-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2"/>
                    </svg>
                    <p class="text-text-secondary">No pool events available</p>
                </div>
            ''')
    except Exception as e:
        return HTMLResponse(content=f'<p class="text-danger-400 py-4">Error loading events: {str(e)}</p>')


@router.get("/pool-history", response_class=HTMLResponse)
async def pool_history(
    request: Request,
    pools: Optional[str] = None,  # Comma-separated pool names
    limit: int = 1000,
    internal: bool = False
):
    """Display zpool history"""
    pool_names = []
    errors = []
    
    try:
        from services.zfs_pool import ZFSPoolService
        pool_service = ZFSPoolService()
        
        # Get list of all available pools
        all_pools = pool_service.list_pools()
        pool_names = [p['name'] for p in all_pools]
    except Exception as e:
        errors.append(f"Failed to list pools: {str(e)}")
    
    # Determine which pools to show
    # Default: show NO data until user explicitly selects pools and applies filters
    if pools:
        selected_pools = [p.strip() for p in pools.split(',') if p.strip()]
    else:
        selected_pools = []
    
    # Get history for all selected pools - organized by pool
    pool_histories = {}
    total_entries = 0
    
    for pool_name in selected_pools:
        if pool_name in pool_names:
            try:
                pool_history = observability_service.get_pool_history(
                    pool_name=pool_name,
                    limit=limit,
                    internal=internal
                )
                pool_histories[pool_name] = pool_history
                total_entries += len(pool_history)
            except Exception as e:
                # Log error but continue with other pools
                errors.append(f"Failed to get history for '{pool_name}': {str(e)}")
                pool_histories[pool_name] = []
    
    return templates.TemplateResponse(
        "zfs/observability/pool_history.jinja",
        {
            "request": request,
            "pool_histories": pool_histories,
            "total_entries": total_entries,
            "all_pools": pool_names,
            "selected_pools": selected_pools,
            "limit": limit,
            "internal": internal,
            "errors": errors if errors else None,
            "page_title": "Pool History"
        }
    )


@router.get("/pool-events", response_class=HTMLResponse)
async def pool_events(
    request: Request,
    pools: Optional[str] = None,  # Comma-separated pool names
    verbose: bool = False
):
    """Display zpool events"""
    pool_names = []
    errors = []
    
    try:
        from services.zfs_pool import ZFSPoolService
        pool_service = ZFSPoolService()
        
        # Get list of all available pools
        all_pools = pool_service.list_pools()
        pool_names = [p['name'] for p in all_pools]
    except Exception as e:
        errors.append(f"Failed to list pools: {str(e)}")
    
    # Determine which pools to show
    # Default: show NO data until user explicitly selects pools and applies filters
    if pools:
        selected_pools = [p.strip() for p in pools.split(',') if p.strip()]
    else:
        selected_pools = []
    
    # Get events for all selected pools - organized by pool
    pool_events_dict = {}
    total_events = 0
    
    for pool_name in selected_pools:
        if pool_name in pool_names:
            try:
                events = observability_service.get_pool_events(
                    pool_name=pool_name,
                    verbose=verbose
                )
                pool_events_dict[pool_name] = events
                total_events += len(events)
            except Exception as e:
                # Log error but continue with other pools
                errors.append(f"Failed to get events for '{pool_name}': {str(e)}")
                pool_events_dict[pool_name] = []
    
    return templates.TemplateResponse(
        "zfs/observability/pool_events.jinja",
        {
            "request": request,
            "pool_events": pool_events_dict,
            "total_events": total_events,
            "all_pools": pool_names,
            "selected_pools": selected_pools,
            "verbose": verbose,
            "errors": errors if errors else None,
            "page_title": "Pool Events"
        }
    )


@router.post("/pool-events/clear", response_class=HTMLResponse)
async def clear_events(request: Request, pool: Optional[str] = Form(None)):
    """Clear pool events"""
    try:
        observability_service.clear_pool_events(pool_name=pool)
        return RedirectResponse(
            url=f"/zfs/observability/pool-events?message=Events cleared successfully",
            status_code=303
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/zfs/observability/pool-events?error={str(e)}",
            status_code=303
        )


@router.get("/kernel-log", response_class=HTMLResponse)
async def kernel_debug_log(
    request: Request,
    lines: int = 1000,
    filter: Optional[str] = None
):
    """Display ZFS kernel debug log"""
    import platform
    from services.utils import is_freebsd, is_netbsd
    
    # Determine the appropriate source description based on platform
    if is_freebsd() or is_netbsd():
        debug_source = "sysctl kstat.zfs.misc.dbgmsg"
    else:
        debug_source = "/proc/spl/kstat/zfs/dbgmsg"
    
    try:
        log_lines = observability_service.get_kernel_debug_log(
            lines=lines,
            filter_pattern=filter
        )
        
        return templates.TemplateResponse(
            "zfs/observability/kernel_log.jinja",
            {
                "request": request,
                "log_lines": log_lines,
                "lines": lines,
                "filter": filter,
                "debug_source": debug_source,
                "page_title": "ZFS Kernel Debug Log"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "zfs/observability/kernel_log.jinja",
            {
                "request": request,
                "log_lines": [f"Error: {str(e)}"],
                "error": str(e),
                "debug_source": debug_source,
                "page_title": "ZFS Kernel Debug Log"
            }
        )


@router.get("/syslog", response_class=HTMLResponse)
async def syslog_zfs(
    request: Request,
    lines: int = 1000,
    severity: Optional[str] = None
):
    """Display ZFS-related syslog entries"""
    try:
        syslog_entries = observability_service.get_syslog_zfs(
            lines=lines,
            severity=severity
        )
        
        return templates.TemplateResponse(
            "zfs/observability/syslog.jinja",
            {
                "request": request,
                "syslog_entries": syslog_entries,
                "lines": lines,
                "severity": severity,
                "filtered": True,
                "page_title": "ZFS System Log"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "zfs/observability/syslog.jinja",
            {
                "request": request,
                "syslog_entries": [],
                "error": str(e),
                "filtered": True,
                "page_title": "ZFS System Log"
            }
        )


@router.get("/syslog-full", response_class=HTMLResponse)
async def syslog_full(
    request: Request,
    lines: int = 1000,
    severity: Optional[str] = None
):
    """Display full unfiltered syslog entries"""
    try:
        # Get unfiltered syslog by calling get_syslog_zfs without ZFS filtering
        import subprocess
        
        cmd = ['journalctl', '-n', str(lines), '--no-pager']
        
        if severity:
            priority_map = {
                'error': 'err',
                'warning': 'warning',
                'info': 'info'
            }
            if severity.lower() in priority_map:
                cmd.extend(['-p', priority_map[severity.lower()]])
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False
        )
        
        syslog_entries = []
        if result.returncode == 0:
            for line in result.stdout.split('\n'):
                if line.strip():
                    syslog_entries.append({'message': line.strip()})
        else:
            # Fallback to dmesg
            result = subprocess.run(
                ['dmesg', '-T'],
                capture_output=True,
                text=True,
                check=False
            )
            if result.returncode == 0:
                for line in result.stdout.split('\n')[-lines:]:
                    if line.strip():
                        syslog_entries.append({'message': line.strip()})
        
        return templates.TemplateResponse(
            "zfs/observability/syslog.jinja",
            {
                "request": request,
                "syslog_entries": syslog_entries,
                "lines": lines,
                "severity": severity,
                "filtered": False,
                "page_title": "Full System Log"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "zfs/observability/syslog.jinja",
            {
                "request": request,
                "syslog_entries": [],
                "error": str(e),
                "filtered": False,
                "page_title": "Full System Log"
            }
        )


@router.get("/arc-summary", response_class=HTMLResponse)
async def arc_summary(request: Request):
    """Display ARC statistics summary"""
    import platform
    
    try:
        system = platform.system()
        
        # All platforms now use the parsed visual dashboard
        # FreeBSD/NetBSD use sysctl, Linux uses /proc/spl/kstat/zfs/arcstats
        arc_stats = observability_service.get_arc_summary()
        
        return templates.TemplateResponse(
            "zfs/observability/arc_summary.jinja",
            {
                "request": request,
                "arc_stats": arc_stats,
                "system": system,
                "page_title": "ARC Summary"
            }
        )
    except Exception as e:
        import platform
        return templates.TemplateResponse(
            "zfs/observability/arc_summary.jinja",
            {
                "request": request,
                "arc_stats": {},
                "system": platform.system(),
                "error": str(e),
                "page_title": "ARC Summary"
            }
        )


@router.get("/module-parameters", response_class=HTMLResponse)
async def module_parameters(request: Request):
    """Display ZFS kernel module parameters"""
    try:
        parameters = observability_service.get_zfs_module_parameters()
        
        return templates.TemplateResponse(
            "zfs/observability/module_parameters.jinja",
            {
                "request": request,
                "parameters": parameters,
                "page_title": "ZFS Module Parameters"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "zfs/observability/module_parameters.jinja",
            {
                "request": request,
                "parameters": {},
                "error": str(e),
                "page_title": "ZFS Module Parameters"
            }
        )


@router.get("/module-parameters/download", response_class=PlainTextResponse)
async def download_module_parameters():
    """Download module parameters as text file"""
    try:
        parameters = observability_service.get_zfs_module_parameters()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Build text output
        output_lines = []
        output_lines.append("=" * 80)
        output_lines.append(f"ZFS Kernel Module Parameters")
        output_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        output_lines.append("=" * 80)
        output_lines.append("")
        
        if parameters.get('error'):
            output_lines.append(f"Error: {parameters['error']}")
        else:
            # Header
            output_lines.append(f"{'Parameter':<50} {'Value':<30}")
            output_lines.append("-" * 80)
            
            # Sort parameters alphabetically
            for key in sorted(parameters.keys()):
                if key != 'error':
                    value = str(parameters[key])
                    output_lines.append(f"{key:<50} {value:<30}")
            
            output_lines.append("")
            output_lines.append(f"Total parameters: {len([k for k in parameters.keys() if k != 'error'])}")
        
        output_lines.append("\n" + "=" * 80)
        output_lines.append("End of Report")
        output_lines.append("=" * 80)
        
        filename = f"zfs_module_parameters_{timestamp}.txt"
        
        return PlainTextResponse(
            content="\n".join(output_lines),
            headers={
                'Content-Disposition': f'attachment; filename="{filename}"'
            }
        )
    except Exception as e:
        return PlainTextResponse(
            content=f"Error generating download: {str(e)}",
            status_code=500
        )


@router.get("/search", response_class=HTMLResponse)
async def search_logs(
    request: Request,
    query: Optional[str] = None,
    source: str = "all",
    limit: int = 100
):
    """Search across all log sources"""
    try:
        results = []
        if query:
            results = observability_service.search_logs(
                query=query,
                source=source,
                limit=limit
            )
        
        return templates.TemplateResponse(
            "zfs/observability/search.jinja",
            {
                "request": request,
                "results": results,
                "query": query,
                "source": source,
                "limit": limit,
                "page_title": "Search ZFS Logs"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "zfs/observability/search.jinja",
            {
                "request": request,
                "results": [],
                "query": query,
                "error": str(e),
                "page_title": "Search ZFS Logs"
            }
        )


# Download endpoints

@router.get("/pool-history/download", response_class=PlainTextResponse)
async def download_pool_history(
    pools: Optional[str] = None,
    limit: int = 1000,
    internal: bool = False
):
    """Download pool history as text file"""
    try:
        from services.zfs_pool import ZFSPoolService
        pool_service = ZFSPoolService()
        
        # Get pool names
        if pools:
            selected_pools = [p.strip() for p in pools.split(',') if p.strip()]
        else:
            all_pools = pool_service.list_pools()
            selected_pools = [p['name'] for p in all_pools]
        
        # Build text output
        output_lines = []
        output_lines.append("=" * 80)
        output_lines.append(f"ZFS Pool History Report")
        output_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        output_lines.append(f"Pools: {', '.join(selected_pools) if selected_pools else 'All'}")
        output_lines.append(f"Limit: {limit} entries per pool")
        output_lines.append(f"Include Internal: {internal}")
        output_lines.append("=" * 80)
        output_lines.append("")
        
        for pool_name in selected_pools:
            try:
                pool_history = observability_service.get_pool_history(
                    pool_name=pool_name,
                    limit=limit,
                    internal=internal
                )
                
                output_lines.append(f"\n{'=' * 80}")
                output_lines.append(f"Pool: {pool_name}")
                output_lines.append(f"{'=' * 80}")
                output_lines.append(f"Total entries: {len(pool_history)}\n")
                
                for entry in pool_history:
                    timestamp = entry.get('timestamp', '')
                    command = entry.get('command', '')
                    user = entry.get('user', '')
                    host = entry.get('host', '')
                    
                    output_lines.append(f"Timestamp: {timestamp}")
                    output_lines.append(f"Command:   {command}")
                    if user:
                        output_lines.append(f"User:      {user}")
                    if host:
                        output_lines.append(f"Host:      {host}")
                    output_lines.append("-" * 80)
                    
            except Exception as e:
                output_lines.append(f"\nError getting history for '{pool_name}': {str(e)}\n")
        
        output_lines.append("\n" + "=" * 80)
        output_lines.append("End of Report")
        output_lines.append("=" * 80)
        
        filename = f"pool_history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        return PlainTextResponse(
            content="\n".join(output_lines),
            headers={
                'Content-Disposition': f'attachment; filename="{filename}"'
            }
        )
    except Exception as e:
        return PlainTextResponse(
            content=f"Error generating download: {str(e)}",
            status_code=500
        )


@router.get("/pool-events/download", response_class=PlainTextResponse)
async def download_pool_events(
    pools: Optional[str] = None,
    verbose: bool = False
):
    """Download pool events as text file"""
    try:
        from services.zfs_pool import ZFSPoolService
        pool_service = ZFSPoolService()
        
        # Get pool names
        if pools:
            selected_pools = [p.strip() for p in pools.split(',') if p.strip()]
        else:
            all_pools = pool_service.list_pools()
            selected_pools = [p['name'] for p in all_pools]
        
        # Build text output
        output_lines = []
        output_lines.append("=" * 80)
        output_lines.append(f"ZFS Pool Events Report")
        output_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        output_lines.append(f"Pools: {', '.join(selected_pools) if selected_pools else 'All'}")
        output_lines.append(f"Verbose: {verbose}")
        output_lines.append("=" * 80)
        output_lines.append("")
        
        for pool_name in selected_pools:
            try:
                events = observability_service.get_pool_events(
                    pool_name=pool_name,
                    verbose=verbose
                )
                
                output_lines.append(f"\n{'=' * 80}")
                output_lines.append(f"Pool: {pool_name}")
                output_lines.append(f"{'=' * 80}")
                output_lines.append(f"Total events: {len(events)}\n")
                
                if verbose:
                    # In verbose mode, use raw output
                    for event in events:
                        output_lines.append(event.get('raw', str(event)))
                        output_lines.append("")
                else:
                    # In normal mode, format as table
                    for event in events:
                        time = event.get('time', '')
                        event_class = event.get('class', '')
                        details = event.get('details', '')
                        
                        output_lines.append(f"Time:    {time}")
                        output_lines.append(f"Class:   {event_class}")
                        if details:
                            output_lines.append(f"Details: {details}")
                        output_lines.append("-" * 80)
                    
            except Exception as e:
                output_lines.append(f"\nError getting events for '{pool_name}': {str(e)}\n")
        
        output_lines.append("\n" + "=" * 80)
        output_lines.append("End of Report")
        output_lines.append("=" * 80)
        
        filename = f"pool_events_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        return PlainTextResponse(
            content="\n".join(output_lines),
            headers={
                'Content-Disposition': f'attachment; filename="{filename}"'
            }
        )
    except Exception as e:
        return PlainTextResponse(
            content=f"Error generating download: {str(e)}",
            status_code=500
        )


@router.get("/arc-summary/download", response_class=PlainTextResponse)
async def download_arc_summary():
    """Download ARC summary as text file"""
    try:
        arc_stats = observability_service.get_arc_summary()
        
        # Build text output
        output_lines = []
        output_lines.append("=" * 80)
        output_lines.append(f"ZFS ARC (Adaptive Replacement Cache) Summary")
        output_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        output_lines.append("=" * 80)
        output_lines.append("")
        
        if arc_stats.get('error'):
            output_lines.append(f"Error: {arc_stats['error']}")
        else:
            # Key metrics
            output_lines.append("KEY METRICS")
            output_lines.append("-" * 80)
            if 'size_human' in arc_stats:
                output_lines.append(f"ARC Size:           {arc_stats['size_human']}")
            if 'c_max_human' in arc_stats:
                output_lines.append(f"ARC Max Size:       {arc_stats['c_max_human']}")
            if 'hit_rate' in arc_stats:
                output_lines.append(f"Hit Rate:           {arc_stats['hit_rate']:.2f}%")
            if 'hits' in arc_stats:
                output_lines.append(f"Cache Hits:         {arc_stats['hits']:,}")
            if 'misses' in arc_stats:
                output_lines.append(f"Cache Misses:       {arc_stats['misses']:,}")
            output_lines.append("")
            
            # All statistics
            output_lines.append("DETAILED STATISTICS")
            output_lines.append("-" * 80)
            
            # Skip special keys
            skip_keys = {'error', 'size_human', 'c_max_human', 'hit_rate'}
            
            for key, value in sorted(arc_stats.items()):
                if key not in skip_keys:
                    output_lines.append(f"{key:.<50} {value}")
        
        output_lines.append("\n" + "=" * 80)
        output_lines.append("End of Report")
        output_lines.append("=" * 80)
        
        filename = f"arc_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        return PlainTextResponse(
            content="\n".join(output_lines),
            headers={
                'Content-Disposition': f'attachment; filename="{filename}"'
            }
        )
    except Exception as e:
        return PlainTextResponse(
            content=f"Error generating download: {str(e)}",
            status_code=500
        )


@router.get("/kernel-log/download", response_class=PlainTextResponse)
async def download_kernel_log(
    lines: int = 1000,
    filter: Optional[str] = None
):
    """Download kernel debug log as text file"""
    try:
        log_lines = observability_service.get_kernel_debug_log(
            lines=lines,
            filter_pattern=filter
        )
        
        # Build text output
        output_lines = []
        output_lines.append("=" * 80)
        output_lines.append(f"ZFS Kernel Debug Log")
        output_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        output_lines.append(f"Lines: {lines}")
        if filter:
            output_lines.append(f"Filter: {filter}")
        output_lines.append("=" * 80)
        output_lines.append("")
        
        output_lines.extend(log_lines)
        
        output_lines.append("\n" + "=" * 80)
        output_lines.append("End of Log")
        output_lines.append("=" * 80)
        
        filename = f"kernel_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        return PlainTextResponse(
            content="\n".join(output_lines),
            headers={
                'Content-Disposition': f'attachment; filename="{filename}"'
            }
        )
    except Exception as e:
        return PlainTextResponse(
            content=f"Error generating download: {str(e)}",
            status_code=500
        )


@router.get("/syslog/download", response_class=PlainTextResponse)
async def download_syslog(
    lines: int = 1000,
    severity: Optional[str] = None
):
    """Download syslog as text file"""
    try:
        syslog_entries = observability_service.get_syslog_zfs(
            lines=lines,
            severity=severity
        )
        
        # Build text output
        output_lines = []
        output_lines.append("=" * 80)
        output_lines.append(f"ZFS System Log (Syslog)")
        output_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        output_lines.append(f"Lines: {lines}")
        if severity:
            output_lines.append(f"Severity: {severity}")
        output_lines.append("=" * 80)
        output_lines.append("")
        
        for entry in syslog_entries:
            output_lines.append(entry.get('message', ''))
        
        output_lines.append("\n" + "=" * 80)
        output_lines.append("End of Log")
        output_lines.append("=" * 80)
        
        filename = f"syslog_zfs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        return PlainTextResponse(
            content="\n".join(output_lines),
            headers={
                'Content-Disposition': f'attachment; filename="{filename}"'
            }
        )
    except Exception as e:
        return PlainTextResponse(
            content=f"Error generating download: {str(e)}",
            status_code=500
        )


@router.get("/download-all")
async def download_all_logs():
    """Download all ZFS observability data as a zip file"""
    try:
        import socket
        from services.zfs_pool import ZFSPoolService
        pool_service = ZFSPoolService()
        
        # Get server hostname
        hostname = socket.gethostname()
        
        # Create an in-memory zip file
        zip_buffer = io.BytesIO()
        
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # Add README
            readme_content = f"""ZFS Observability Data Export
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

This archive contains the following files:
1. pool_history.txt - Command history for all pools
2. pool_events.txt - Events for all pools
3. arc_summary.txt - ARC cache statistics
4. kernel_log.txt - ZFS kernel debug log
5. syslog.txt - ZFS-related system log entries
6. module_parameters.txt - ZFS kernel module parameters

Each file contains detailed information about various aspects of your ZFS system.
"""
            zip_file.writestr('README.txt', readme_content)
            
            # 1. Pool History
            try:
                all_pools = pool_service.list_pools()
                selected_pools = [p['name'] for p in all_pools]
                
                output_lines = []
                output_lines.append("=" * 80)
                output_lines.append(f"ZFS Pool History Report")
                output_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                output_lines.append("=" * 80)
                output_lines.append("")
                
                for pool_name in selected_pools:
                    try:
                        pool_history = observability_service.get_pool_history(
                            pool_name=pool_name,
                            limit=5000,
                            internal=False
                        )
                        
                        output_lines.append(f"\n{'=' * 80}")
                        output_lines.append(f"Pool: {pool_name}")
                        output_lines.append(f"{'=' * 80}")
                        output_lines.append(f"Total entries: {len(pool_history)}\n")
                        
                        for entry in pool_history:
                            timestamp = entry.get('timestamp', '')
                            command = entry.get('command', '')
                            user = entry.get('user', '')
                            host = entry.get('host', '')
                            
                            output_lines.append(f"Timestamp: {timestamp}")
                            output_lines.append(f"Command:   {command}")
                            if user:
                                output_lines.append(f"User:      {user}")
                            if host:
                                output_lines.append(f"Host:      {host}")
                            output_lines.append("-" * 80)
                    except Exception as e:
                        output_lines.append(f"\nError getting history for '{pool_name}': {str(e)}\n")
                
                zip_file.writestr('pool_history.txt', "\n".join(output_lines))
            except Exception as e:
                zip_file.writestr('pool_history.txt', f"Error: {str(e)}")
            
            # 2. Pool Events
            try:
                all_pools = pool_service.list_pools()
                selected_pools = [p['name'] for p in all_pools]
                
                output_lines = []
                output_lines.append("=" * 80)
                output_lines.append(f"ZFS Pool Events Report")
                output_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                output_lines.append("=" * 80)
                output_lines.append("")
                
                for pool_name in selected_pools:
                    try:
                        events = observability_service.get_pool_events(
                            pool_name=pool_name,
                            verbose=True
                        )
                        
                        output_lines.append(f"\n{'=' * 80}")
                        output_lines.append(f"Pool: {pool_name}")
                        output_lines.append(f"{'=' * 80}")
                        output_lines.append(f"Total events: {len(events)}\n")
                        
                        for event in events:
                            output_lines.append(event.get('raw', str(event)))
                            output_lines.append("")
                    except Exception as e:
                        output_lines.append(f"\nError getting events for '{pool_name}': {str(e)}\n")
                
                zip_file.writestr('pool_events.txt', "\n".join(output_lines))
            except Exception as e:
                zip_file.writestr('pool_events.txt', f"Error: {str(e)}")
            
            # 3. ARC Summary
            try:
                arc_stats = observability_service.get_arc_summary()
                
                output_lines = []
                output_lines.append("=" * 80)
                output_lines.append(f"ZFS ARC (Adaptive Replacement Cache) Summary")
                output_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                output_lines.append("=" * 80)
                output_lines.append("")
                
                if arc_stats.get('error'):
                    output_lines.append(f"Error: {arc_stats['error']}")
                else:
                    output_lines.append("KEY METRICS")
                    output_lines.append("-" * 80)
                    if 'size_human' in arc_stats:
                        output_lines.append(f"ARC Size:           {arc_stats['size_human']}")
                    if 'c_max_human' in arc_stats:
                        output_lines.append(f"ARC Max Size:       {arc_stats['c_max_human']}")
                    if 'hit_rate' in arc_stats:
                        output_lines.append(f"Hit Rate:           {arc_stats['hit_rate']:.2f}%")
                    if 'hits' in arc_stats:
                        output_lines.append(f"Cache Hits:         {arc_stats['hits']:,}")
                    if 'misses' in arc_stats:
                        output_lines.append(f"Cache Misses:       {arc_stats['misses']:,}")
                    output_lines.append("")
                    
                    output_lines.append("DETAILED STATISTICS")
                    output_lines.append("-" * 80)
                    
                    skip_keys = {'error', 'size_human', 'c_max_human', 'hit_rate'}
                    
                    for key, value in sorted(arc_stats.items()):
                        if key not in skip_keys:
                            output_lines.append(f"{key:.<50} {value}")
                
                zip_file.writestr('arc_summary.txt', "\n".join(output_lines))
            except Exception as e:
                zip_file.writestr('arc_summary.txt', f"Error: {str(e)}")
            
            # 4. Kernel Log
            try:
                log_lines = observability_service.get_kernel_debug_log(
                    lines=5000,
                    filter_pattern=None
                )
                
                output_lines = []
                output_lines.append("=" * 80)
                output_lines.append(f"ZFS Kernel Debug Log")
                output_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                output_lines.append("=" * 80)
                output_lines.append("")
                output_lines.extend(log_lines)
                
                zip_file.writestr('kernel_log.txt', "\n".join(output_lines))
            except Exception as e:
                zip_file.writestr('kernel_log.txt', f"Error: {str(e)}")
            
            # 5. Syslog
            try:
                syslog_entries = observability_service.get_syslog_zfs(
                    lines=5000,
                    severity=None
                )
                
                output_lines = []
                output_lines.append("=" * 80)
                output_lines.append(f"ZFS System Log (Syslog)")
                output_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                output_lines.append("=" * 80)
                output_lines.append("")
                
                for entry in syslog_entries:
                    output_lines.append(entry.get('message', ''))
                
                zip_file.writestr('syslog.txt', "\n".join(output_lines))
            except Exception as e:
                zip_file.writestr('syslog.txt', f"Error: {str(e)}")
            
            # 6. Module Parameters
            try:
                parameters = observability_service.get_zfs_module_parameters()
                
                output_lines = []
                output_lines.append("=" * 80)
                output_lines.append(f"ZFS Kernel Module Parameters")
                output_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                output_lines.append("=" * 80)
                output_lines.append("")
                
                if parameters.get('error'):
                    output_lines.append(f"Error: {parameters['error']}")
                else:
                    for key, value in sorted(parameters.items()):
                        if key != 'error':
                            output_lines.append(f"{key:.<50} {value}")
                
                zip_file.writestr('module_parameters.txt', "\n".join(output_lines))
            except Exception as e:
                zip_file.writestr('module_parameters.txt', f"Error: {str(e)}")
        
        # Prepare the zip for download
        zip_buffer.seek(0)
        
        filename = f"zfs_observability_{hostname}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
        
        return StreamingResponse(
            zip_buffer,
            media_type="application/zip",
            headers={
                'Content-Disposition': f'attachment; filename="{filename}"'
            }
        )
        
    except Exception as e:
        return PlainTextResponse(
            content=f"Error generating zip file: {str(e)}",
            status_code=500
        )
