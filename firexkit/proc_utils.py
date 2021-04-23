from datetime import timedelta
from getpass import getuser
import psutil
import re
import time

from celery.utils.log import get_task_logger


logger = get_task_logger(__name__)


def get_firex_id_from_cmdline(cmd_line: []):
    matches = []
    firex_uid_dir = r'FireX\-\w+\-\d+\-\d+\-\d+'
    for i in cmd_line:
        found = re.findall(firex_uid_dir, i)
        matches += found
    return list(set(matches))


def kill_old_procs(proc_name, keepalive=2*24*60*60, regexstr=None):
    all_firex_ids = []
    for proc in find_current_user_recent_procs(proc_name, keepalive, regexstr):
        try:
            msg = 'Killing %s' % proc.pid
            firex_ids = get_firex_id_from_cmdline(proc.cmdline())
            all_firex_ids += firex_ids
            if firex_ids:
                msg += ' [%s]' % ', '.join(firex_ids)
            logger.debug(msg)
            proc.kill()
        except Exception as e:
            logger.debug('------ FAILED %r' % e)
    return list(set(all_firex_ids))


def find_current_user_recent_procs(proc_name, max_age=2 * 24 * 60 * 60, regexstr=None):

    proclist = []
    if regexstr:
        regex = re.compile(regexstr)
    else:
        regex = None
    first_time = True
    username = getuser()
    for proc in psutil.process_iter():
        try:
            pinfo = proc.as_dict(attrs=['name', 'cmdline', 'pid', 'username'])
        except psutil.NoSuchProcess:
            pass
        except IndexError as e:
            # psutil can fail with an IndexError when attempting to inspect some username values, such as nginx
            # worker processes launched via docker/supodman.
            logger.warning(f"Failure trying to read process details: {e}")
        else:
            if pinfo['name'] == proc_name:
                if regex:
                    match_found = False
                    for item in pinfo['cmdline']:
                        match = regex.search(item)
                        if match:
                            match_found = True
                            logger.debug('Match Object for %s found in %s' % (regexstr, item))
                            break
                else:
                    match_found = True
                if match_found:
                    create_time = proc.create_time()
                    elapsed = time.time() - create_time
                    if elapsed >= max_age:
                        if first_time:
                            keepalive_str = str(timedelta(seconds=max_age))
                            logger.debug('[%s] Processes older than %s:' % (pinfo['name'], keepalive_str))
                            first_time = False

                        elapsed_str = str(timedelta(seconds=elapsed))
                        if pinfo['username'] == username:
                            proclist.append(proc)
                            logger.debug('---- pid %s has been active for %s; cmdline=%s' %
                                         (pinfo['pid'], elapsed_str, pinfo['cmdline']))
                        else:
                            logger.debug('---- pid %s has been active for %s but owned by %s, cmdline=%s' %
                                         (pinfo['pid'], elapsed_str, pinfo['username'], pinfo['cmdline']))
    return proclist
