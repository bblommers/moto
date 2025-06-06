from __future__ import annotations

import copy
import logging
import threading
from typing import Any, Dict, Final, List, Optional

from moto.stepfunctions.parser.api import (
    Arn,
    ExecutionFailedEventDetails,
    StateMachineType,
    Timestamp,
)
from moto.stepfunctions.parser.asl.component.state.exec.state_map.iteration.itemprocessor.map_run_record import (
    MapRunRecordPoolManager,
)
from moto.stepfunctions.parser.asl.eval.callback.callback import CallbackPoolManager
from moto.stepfunctions.parser.asl.eval.evaluation_details import AWSExecutionDetails
from moto.stepfunctions.parser.asl.eval.event.event_manager import (
    EventHistoryContext,
    EventManager,
)
from moto.stepfunctions.parser.asl.eval.event.logging import (
    CloudWatchLoggingSession,
)
from moto.stepfunctions.parser.asl.eval.program_state import (
    ProgramEnded,
    ProgramError,
    ProgramRunning,
    ProgramState,
    ProgramStopped,
    ProgramTimedOut,
)
from moto.stepfunctions.parser.asl.eval.states import ContextObjectData, States
from moto.stepfunctions.parser.asl.eval.variable_store import VariableStore
from moto.stepfunctions.parser.backend.activity import Activity

LOG = logging.getLogger(__name__)


class Environment:
    _state_mutex: Final[threading.RLock()]
    _program_state: Optional[ProgramState]
    program_state_event: Final[threading.Event()]

    event_manager: EventManager
    event_history_context: Final[EventHistoryContext]
    cloud_watch_logging_session: Optional[CloudWatchLoggingSession]
    aws_execution_details: Final[AWSExecutionDetails]
    execution_type: StateMachineType
    callback_pool_manager: CallbackPoolManager
    map_run_record_pool_manager: MapRunRecordPoolManager
    activity_store: Dict[Arn, Activity]

    _frames: Final[List[Environment]]
    _is_frame: bool = False

    heap: Dict[str, Any] = dict()
    stack: List[Any] = list()
    states: States
    variable_store: VariableStore

    def __init__(
        self,
        aws_execution_details: AWSExecutionDetails,
        execution_type: StateMachineType,
        context: ContextObjectData,
        event_history_context: EventHistoryContext,
        cloud_watch_logging_session: Optional[CloudWatchLoggingSession],
        activity_store: Dict[Arn, Activity],
        variable_store: Optional[VariableStore] = None,
    ):
        super(Environment, self).__init__()
        self._state_mutex = threading.RLock()
        self._program_state = None
        self.program_state_event = threading.Event()

        self.cloud_watch_logging_session = cloud_watch_logging_session
        self.event_manager = EventManager(
            cloud_watch_logging_session=cloud_watch_logging_session
        )
        self.event_history_context = event_history_context

        self.aws_execution_details = aws_execution_details
        self.execution_type = execution_type
        self.callback_pool_manager = CallbackPoolManager(activity_store=activity_store)
        self.map_run_record_pool_manager = MapRunRecordPoolManager()

        self.activity_store = activity_store

        self._frames = list()
        self._is_frame = False

        self.heap = dict()
        self.stack = list()
        self.states = States(context=context)
        self.variable_store = variable_store or VariableStore()

    @classmethod
    def as_frame_of(
        cls,
        env: Environment,
        event_history_frame_cache: Optional[EventHistoryContext] = None,
    ) -> Environment:
        return Environment.as_inner_frame_of(
            env=env,
            variable_store=env.variable_store,
            event_history_frame_cache=event_history_frame_cache,
        )

    @classmethod
    def as_inner_frame_of(
        cls,
        env: Environment,
        variable_store: VariableStore,
        event_history_frame_cache: Optional[EventHistoryContext] = None,
    ) -> Environment:
        # Construct the frame's context object data.
        context = ContextObjectData(
            Execution=env.states.context_object.context_object_data["Execution"],
            StateMachine=env.states.context_object.context_object_data["StateMachine"],
        )
        if "Task" in env.states.context_object.context_object_data:
            context["Task"] = env.states.context_object.context_object_data["Task"]

        # The default logic provisions for child frame to extend the source frame event id.
        if event_history_frame_cache is None:
            event_history_frame_cache = EventHistoryContext(
                previous_event_id=env.event_history_context.source_event_id
            )

        frame = cls(
            aws_execution_details=env.aws_execution_details,
            execution_type=env.execution_type,
            context=context,
            event_history_context=event_history_frame_cache,
            cloud_watch_logging_session=env.cloud_watch_logging_session,
            activity_store=env.activity_store,
            variable_store=variable_store,
        )
        frame._is_frame = True
        frame.event_manager = env.event_manager
        if "State" in env.states.context_object.context_object_data:
            frame.states.context_object.context_object_data["State"] = copy.deepcopy(
                env.states.context_object.context_object_data["State"]
            )
        frame.callback_pool_manager = env.callback_pool_manager
        frame.map_run_record_pool_manager = env.map_run_record_pool_manager
        frame.heap = env.heap
        frame._program_state = copy.deepcopy(env._program_state)
        return frame

    @property
    def next_state_name(self) -> Optional[str]:
        next_state_name: Optional[str] = None
        if isinstance(self._program_state, ProgramRunning):
            next_state_name = self._program_state.next_state_name
        return next_state_name

    @next_state_name.setter
    def next_state_name(self, next_state_name: str) -> None:
        if self._program_state is None:
            self._program_state = ProgramRunning()

        if isinstance(self._program_state, ProgramRunning):
            self._program_state.next_state_name = next_state_name
        else:
            raise RuntimeError(
                f"Could not set NextState value when in state '{type(self._program_state)}'."
            )

    def program_state(self) -> ProgramState:
        return copy.deepcopy(self._program_state)

    def is_running(self) -> bool:
        return isinstance(self._program_state, ProgramRunning)

    def set_ended(self) -> None:
        with self._state_mutex:
            if isinstance(self._program_state, ProgramRunning):
                self._program_state = ProgramEnded()
                for frame in self._frames:
                    frame.set_ended()
            self.program_state_event.set()
            self.program_state_event.clear()

    def set_error(self, error: ExecutionFailedEventDetails) -> None:
        with self._state_mutex:
            self._program_state = ProgramError(error=error)
            for frame in self._frames:
                frame.set_error(error=error)
            self.program_state_event.set()
            self.program_state_event.clear()

    def set_timed_out(self) -> None:
        with self._state_mutex:
            self._program_state = ProgramTimedOut()
            for frame in self._frames:
                frame.set_timed_out()
            self.program_state_event.set()
            self.program_state_event.clear()

    def set_stop(
        self, stop_date: Timestamp, cause: Optional[str], error: Optional[str]
    ) -> None:
        with self._state_mutex:
            if isinstance(self._program_state, ProgramRunning):
                self._program_state = ProgramStopped(
                    stop_date=stop_date, cause=cause, error=error
                )
                for frame in self._frames:
                    frame.set_stop(stop_date=stop_date, cause=cause, error=error)
                self.program_state_event.set()
                self.program_state_event.clear()

    def open_frame(
        self, event_history_context: Optional[EventHistoryContext] = None
    ) -> Environment:
        with self._state_mutex:
            frame = self.as_frame_of(
                env=self, event_history_frame_cache=event_history_context
            )
            self._frames.append(frame)
            return frame

    def open_inner_frame(
        self, event_history_context: Optional[EventHistoryContext] = None
    ) -> Environment:
        with self._state_mutex:
            variable_store = VariableStore.as_inner_scope_of(
                outer_variable_store=self.variable_store
            )
            frame = self.as_inner_frame_of(
                env=self,
                variable_store=variable_store,
                event_history_frame_cache=event_history_context,
            )
            self._frames.append(frame)
            return frame

    def close_frame(self, frame: Environment) -> None:
        with self._state_mutex:
            if frame in self._frames:
                self._frames.remove(frame)
                self.event_history_context.integrate(frame.event_history_context)

    def delete_frame(self, frame: Environment) -> None:
        with self._state_mutex:
            if frame in self._frames:
                self._frames.remove(frame)

    def is_frame(self) -> bool:
        return self._is_frame

    def is_standard_workflow(self) -> bool:
        return self.execution_type == StateMachineType.STANDARD
