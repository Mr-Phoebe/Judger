#!/usr/bin/python
import errno
import logging
import os
import signal
import sys
import threading
import traceback
from multiprocessing import Process
from functools import partial
from itertools import chain

from dmoj import packet, graders
from dmoj.config import ConfigNode
from dmoj.control import JudgeControlRequestHandler
from dmoj.error import CompileError
from dmoj.judgeenv import env, get_supported_problems, startup_warnings
# from dmoj.monitor import Monitor, DummyMonitor
from dmoj.problem import Problem, BatchedTestCase
from dmoj.result import Result
from dmoj.utils.ansi import ansi_style, strip_ansi
from dmoj.utils.debugger import setup_all_debuggers
from dmoj.executors import executors
import nsq
import json

setup_all_debuggers()

if os.name == 'posix':
    try:
        import readline
    except ImportError:
        pass


class BatchBegin(object):
    pass


class BatchEnd(object):
    pass


class TerminateGrading(Exception):
    pass


TYPE_SUBMISSION = 1
TYPE_INVOCATION = 2


class Judge(object):
    def __init__(self):
        self.current_submission = None
        self.current_grader = None
        self.current_submission_thread = None
        self._terminate_grading = False
        self.process_type = 0
        self.packet_manager = packet.PacketManager(env['server_url'], env['key'])
        self.url = env['nsq_url']
        self.key = env['judge_key']

        # self.begin_grading = partial(self.process_submission, TYPE_SUBMISSION, self._begin_grading)
        # self.custom_invocation = partial(self.process_submission, TYPE_INVOCATION, self._custom_invocation)

    def update_problems(self):
        """
        Pushes current problem set to server.
        """
        self.packet_manager.supported_problems_packet(get_supported_problems())

    def process_submission(self, type, target, id, *args, **kwargs):
        try:
            self.current_submission_thread.join()
        except AttributeError:
            pass
        self.process_type = type
        self.current_submission = id
        self.current_submission_thread = threading.Thread(target=target, args=args)
        self.current_submission_thread.daemon = True
        self.current_submission_thread.start()
        if kwargs.pop('blocking', False):
            self.current_submission_thread.join()

    def _custom_invocation(self, language, source, memory_limit, time_limit, input_data):
        class InvocationGrader(graders.StandardGrader):
            def check_result(self, case, result):
                return not result.result_flag

        class InvocationProblem(object):
            id = 'CustomInvocation'
            time_limit = time_limit
            memory_limit = memory_limit

        class InvocationCase(object):
            config = ConfigNode({'unbuffered': False})
            io_redirects = lambda: None
            input_data = lambda: input_data

        grader = self.get_grader_from_source(InvocationGrader, InvocationProblem(), language, source)
        binary = grader.binary if grader else None

        if binary:
            self.packet_manager.invocation_begin_packet()
            try:
                result = grader.grade(InvocationCase())
            except TerminateGrading:
                self.packet_manager.submission_terminated_packet()
                print ansi_style('#ansi[Forcefully terminating invocation.](red|bold)')
                pass
            except:
                self.internal_error()
            else:
                self.packet_manager.invocation_end_packet(result)

        print ansi_style('Done invoking #ansi[%s](green|bold).\n' % (id))
        self._terminate_grading = False
        self.current_submission_thread = None
        self.current_submission = None

    def begin_grading(self, problem_id, language, source, time_limit, memory_limit,
            problem_data):
        submission_id = self.current_submission
        print ansi_style('Start grading #ansi[%s](yellow)/#ansi[%s](green|bold) in %s...'
                         % (problem_id, submission_id, language))

        try:
            problem = Problem(problem_id, time_limit, memory_limit, problem_data)
            print "end problem"
        except Exception:
            return self.internal_error()

        # grader_class = graders.StandardGrader

        # grader = self.get_grader_from_source(grader_class, problem, language, source)
        # binary = grader.binary if grader else None

        # the compiler may have failed, or an error could have happened while initializing a custom judge
        # either way, we can't continue
        print "before process"
        try:
            process = self.get_process_from_source(problem, language, source) 
            print process.__dict__
        except Exception as ex:
            print "error :", ex
        return
        if binary:
            self.packet_manager.begin_grading_packet(problem.is_pretested, submission_id)

            batch_counter = 1
            in_batch = False

            # cases are indexed at 1
            case_number = 1
            try:
                for result in self.grade_cases(grader, problem.cases, short_circuit=short_circuit):
                    if isinstance(result, BatchBegin):
                        self.packet_manager.batch_begin_packet(submission_id)
                        print ansi_style("#ansi[Batch #%d](yellow|bold)" % batch_counter)
                        in_batch = True
                    elif isinstance(result, BatchEnd):
                        self.packet_manager.batch_end_packet(submission_id)
                        batch_counter += 1
                        in_batch = False
                    else:
                        codes = result.readable_codes()

                        # here be cancer
                        is_sc = (result.result_flag & Result.SC)
                        colored_codes = map(lambda x: '#ansi[%s](%s|bold)' % ('--' if x == 'SC' else x,
                                                                              Result.COLORS_BYID[x]), codes)
                        colored_aux_codes = '{%s}' % ', '.join(colored_codes[1:]) if len(codes) > 1 else ''
                        colored_feedback = '(#ansi[%s](|underline)) ' % result.feedback if result.feedback else ''
                        case_info = '[%.3fs | %dkb] %s%s' % (result.execution_time, result.max_memory,
                                                             colored_feedback,
                                                             colored_aux_codes) if not is_sc else ''
                        case_padding = '  ' * in_batch
                        print ansi_style('%sTest case %2d %-3s %s' % (case_padding, case_number,
                                                                      colored_codes[0], case_info))

                        self.packet_manager.test_case_status_packet(case_number, result)

                        case_number += 1
            except TerminateGrading:
                self.packet_manager.submission_terminated_packet()
                print ansi_style('#ansi[Forcefully terminating grading. Temporary files may not be deleted.](red|bold)')
                pass
            except:
                self.internal_error()
            else:
                self.packet_manager.grading_end_packet()

        print ansi_style('Done grading #ansi[%s](yellow)/#ansi[%s](green|bold).' % (problem_id, submission_id))
        self._terminate_grading = False
        self.current_submission_thread = None
        self.current_submission = None
        self.current_grader = None

    def grade_cases(self, grader, cases, short_circuit=False, is_short_circuiting=False):
        for case in cases:
            # Yield notifying objects for batch begin/end, and unwrap all cases inside the batches
            if isinstance(case, BatchedTestCase):
                yield BatchBegin()
                for batched_case in self.grade_cases(grader, case.batched_cases, short_circuit=True,
                                          is_short_circuiting=is_short_circuiting):
                    if (batched_case.result_flag & Result.WA) > 0 and not case.points:
                        is_short_circuiting = True
                    yield batched_case
                yield BatchEnd()
                continue

            # Stop grading if we're short circuiting
            if is_short_circuiting:
                result = Result(case)
                result.result_flag = Result.SC
                yield result
                continue

            # Must check here because we might be interrupted mid-execution
            # If we don't bail out, we get an IR.
            # In Java's case, all the code after this will crash.
            if self._terminate_grading:
                raise TerminateGrading()

            result = grader.grade(case)

            # If the WA bit of result_flag is set and we are set to short-circuit (e.g., in a batch),
            # short circuit the rest of the cases.
            # Do the same if the case is a pretest (i.e. has 0 points)
            if (result.result_flag & Result.WA) > 0 and (short_circuit or not case.points):
                is_short_circuiting = True

            yield result

    def get_grader_from_source(self, grader_class, problem, language, source):
        if isinstance(source, unicode):
            source = source.encode('utf-8')

        try:
            grader = grader_class(self, problem, language, source)
        except CompileError as ce:
            print ansi_style('#ansi[Failed compiling submission!](red|bold)')
            print ce.message,  # don't print extra newline
            grader = None
        except:  # if custom grader failed to initialize, report it to the site
            return self.internal_error()

        return grader

    def get_process_from_source(self, problem, language, source):
        print "start get executor"
        if isinstance(source, unicode):
            source = source.encode('utf-8')
        print executors.keys()

        executor = executors[language].Executor('validator', source)
        print "got executor"
        return executor.launch(time=problem.time_limit, memory=problem.memory_limit)

       

    def get_process_type(self):
        return {0: None,
                TYPE_SUBMISSION: 'submission',
                TYPE_INVOCATION: 'invocation',
                #   TYPE_HACK:       'hack',
                }[self.process_type]

    def internal_error(self, exc=None):
        # If exc is exists, raise it so that sys.exc_info() is populated with its data
        if exc:
            try:
                raise exc
            except:
                pass
        exc = sys.exc_info()

        message = ''.join(traceback.format_exception(*exc))

        # Strip ANSI from the message, since this might be a checker's CompileError
        # ...we don't want to see the raw ANSI codes from GCC/Clang on the site.
        # We could use format_ansi and send HTML to the site, but the site doesn't presently support HTML
        # internal error formatting.
        self.packet_manager.internal_error_packet(strip_ansi(message))

        # Logs can contain ANSI, and it'll display fine
        print >> sys.stderr, message

    def terminate_grading(self):
        """
        Forcefully terminates the current submission. Not necessarily safe.
        """
        if self.current_submission_thread:
            self._terminate_grading = True
            if self.current_grader:
                self.current_grader.terminate()
            self.current_submission_thread.join()
            self.current_submission_thread = None

    def start_judge(self, message):

        #try:
        params = json.loads(message.body)
        print params
        self.current_submission = int(params['submission_id'])
        print "before start"
        problem_id = int(params['problem_id'])
        print problem_id
        language = params['language']
        print language
        source = params['source']
        print source
        time_limit = int(params['time_limit'])
        print time_limit
        memory_limit = int(params['memory_limit'])
        print memory_limit
        problem_data = params['problem_data']
        print problem_data
        pretests_only=False
        self.begin_grading(problem_id, language, source, time_limit, \
                memory_limit, problem_data)
        #except Exception as ex:
        #    print "error: ", ex

        return True




    def listen(self):
        """
        Attempts to connect to the handler server specified in command line.
        """
        nsq.Reader(message_handler=self.start_judge, 
            nsqd_tcp_addresses=[self.url],
            topic='judge', channel=self.key, 
            lookupd_poll_interval=15)
        nsq.run()


    def __del__(self):
        del self.packet_manager

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        pass

    def murder(self):
        """
        End any submission currently executing, and exit the judge.
        """
        self.terminate_grading()


class ClassicJudge(Judge):
    def __init__(self, url):
        super(ClassicJudge, self).__init__()


def sanity_check():
    # Don't allow starting up without wbox/cptbox, saves cryptic errors later on
    if os.name == 'nt':
        try:
            import wbox
        except ImportError:
            print >> sys.stderr, "wbox must be compiled to grade!"
            return False

        # DMOJ needs to be run as admin on Windows
        import ctypes
        if ctypes.windll.shell32.IsUserAnAdmin() == 0:
            print >> sys.stderr, "can't start, the DMOJ judge must be ran as admin"
            return False
    else:
        try:
            import cptbox
        except ImportError:
            print >> sys.stderr, "cptbox must be compiled to grade!"
            return False

        # However running as root on Linux is a Bad Idea
        if os.getuid() == 0:
            startup_warnings.append('running the judge as root can be potentially unsafe, '
                                    'consider using an unprivileged user instead')

    # _checker implements standard checker functions in C
    # we fall back to a Python implementation if it's not compiled, but it's slower
    try:
        from checkers import _checker
    except ImportError:
        startup_warnings.append('native checker module not found, compile _checker for optimal performance')
    return True

def judge_proc():
    global g_judge
    from dmoj import judgeenv

    logfile = judgeenv.log_file

    try:
        logfile = logfile % env['id']
    except TypeError:
        pass

    logging.basicConfig(filename=logfile, level=logging.INFO,
                        format='%(levelname)s %(asctime)s %(module)s %(message)s')

    g_judge = Judge()
    g_judge.listen()

    if hasattr(signal, 'SIGUSR2'):
        def update_problem_signal(signum, frame):
            g_judge.update_problems()

        signal.signal(signal.SIGUSR2, update_problem_signal)



PR_SET_PDEATHSIG = 1


def main():  # pragma: no cover
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
    if not sanity_check():
        return 1

    from dmoj import judgeenv, executors

    judgeenv.load_env()

    # Emulate ANSI colors with colorama
    if os.name == 'nt' and not judgeenv.no_ansi_emu:
        try:
            from colorama import init
            init()
        except ImportError:
            pass

    executors.load_executors()

    print 'Running live judge...'

    for warning in judgeenv.startup_warnings:
        print ansi_style('#ansi[Warning: %s](yellow)' % warning)
    del judgeenv.startup_warnings

    judge_proc()

if __name__ == '__main__':
    main()
