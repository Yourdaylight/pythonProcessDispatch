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
        ready_processes = [process for process in self.processes if
                           cur_event.event_time >= process.arrival_time and process.process_state == ProcessStates.READY]
        ready_processes.sort(key=lambda process:  ((process.service_time - process.remaining_time),process.arrival_time))
        if len(ready_processes) > 0:
            # Round Robin
            return ready_processes[0]

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
        ready_processes = [process for process in self.processes if
                           process.process_state == ProcessStates.READY]
        ready_processes.sort(key=lambda process:  ((process.service_time - process.remaining_time),process.arrival_time))
        start_time = cur_event.event_time
        end_time = ready_processes[0].remaining_time + start_time
        for process in ready_processes:
            if process.arrival_time <= end_time:
                new_end_time = min(end_time, cur_event.event_time + process.remaining_time)
                if new_end_time != end_time:
                    self.quantum = new_end_time - start_time
                    end_time = new_end_time
        return ready_processes[0]

    def dispatcher_func(self, cur_process):
        self.process_on_cpu.process_state = ProcessStates.RUNNING
        execute_time = self.time + cur_process.run_for(self.quantum, self.time)
        if cur_process.remaining_time > 0:
            self.process_on_cpu.process_state = ProcessStates.READY
            self.quantum = math.inf
            return Event(process_id=self.process_on_cpu.process_id, event_type=EventTypes.PROC_CPU_REQ, event_time=execute_time)
        else:
            self.process_on_cpu.process_state = ProcessStates.TERMINATED
            self.quantum = math.inf
            return Event(process_id=self.process_on_cpu.process_id, event_type=EventTypes.PROC_CPU_DONE, event_time=execute_time)
