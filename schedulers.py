import math

from des import SchedulerDES
from event import Event, EventTypes
from process import ProcessStates


class FCFS(SchedulerDES):
    def scheduler_func(self, cur_event):
        if cur_event.event_type == EventTypes.PROC_ARRIVES:
            process = self.processes[cur_event.process_id]
            if process.arrival_time <= self.time and process.process_state == ProcessStates.READY:
                process.process_state = ProcessStates.READY
                return process

    def dispatcher_func(self, cur_process):
        self.process_on_cpu.process_state = ProcessStates.RUNNING
        execute_time = self.time + cur_process.run_for(self.quantum, self.time)

        self.process_on_cpu.process_state = ProcessStates.TERMINATED
        return Event(process_id=self.process_on_cpu.process_id, event_type=EventTypes.PROC_CPU_DONE, event_time=execute_time)


class SJF(SchedulerDES):
    def scheduler_func(self, cur_event):
        ready_processes = [process for process in self.processes if
                           self.time >= process.arrival_time and process.process_state == ProcessStates.READY]
        if len(ready_processes) > 0:
            return min(ready_processes, key=lambda process: process.remaining_time)

    def dispatcher_func(self, cur_process):
        self.process_on_cpu.process_state = ProcessStates.RUNNING
        execute_time = self.time + cur_process.run_for(self.quantum, self.time)

        self.process_on_cpu.process_state = ProcessStates.TERMINATED
        return Event(process_id=self.process_on_cpu.process_id, event_type=EventTypes.PROC_CPU_DONE, event_time=execute_time)


class RR(SchedulerDES):
    def scheduler_func(self, cur_event):
        all_events = [cur_event]
        for event in self.events_queue:
            if event.event_time <= self.time:
                if self.processes[event.process_id].process_state == ProcessStates.READY:
                    all_events.append(event)
        all_events.sort(key=lambda _event: _event.event_time)
        return self.processes[all_events[0].process_id]

    def dispatcher_func(self, cur_process):
        self.process_on_cpu.process_state = ProcessStates.RUNNING
        execute_time = self.time + cur_process.run_for(self.quantum, self.time)
        if cur_process.remaining_time > 0:
            self.process_on_cpu.process_state = ProcessStates.READY
            return Event(process_id=self.process_on_cpu.process_id, event_type=EventTypes.PROC_CPU_REQ, event_time=execute_time)
        else:
            self.process_on_cpu.process_state = ProcessStates.TERMINATED
            return Event(process_id=self.process_on_cpu.process_id, event_type=EventTypes.PROC_CPU_DONE, event_time=execute_time)


class SRTF(SchedulerDES):
    def scheduler_func(self, cur_event):
        finish_time = self.process_on_cpu.remaining_time + self.time if self.process_on_cpu else self.processes[
                                                                                                     cur_event.process_id].remaining_time + self.time
        ready_processes = [process for process in self.processes if
                           process.arrival_time <= finish_time and process.process_state != ProcessStates.TERMINATED]
        ready_processes.sort(key=lambda process: process.remaining_time)
        rtn_process = None
        for process in ready_processes:
            if process.process_state == ProcessStates.NEW:
                self.quantum = process.arrival_time - cur_event.event_time
            elif process.process_state == ProcessStates.READY:
                if rtn_process is None:
                    rtn_process = process
                if self.process_on_cpu:
                    if process.remaining_time < self.process_on_cpu.remaining_time:
                        self.quantum = process.remaining_time
                        rtn_process = process
        return rtn_process

    def dispatcher_func(self, cur_process):
        self.process_on_cpu.process_state = ProcessStates.RUNNING
        execute_time = self.time + cur_process.run_for(self.quantum, self.time)
        if cur_process.remaining_time > 0:
            self.process_on_cpu.process_state = ProcessStates.READY
            return Event(process_id=self.process_on_cpu.process_id, event_type=EventTypes.PROC_CPU_REQ, event_time=execute_time)
        else:
            self.process_on_cpu.process_state = ProcessStates.TERMINATED
            return Event(process_id=self.process_on_cpu.process_id, event_type=EventTypes.PROC_CPU_DONE, event_time=execute_time)
