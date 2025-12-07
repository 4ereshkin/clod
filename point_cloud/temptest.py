import asyncio
from datetime import timedelta
from temporalio import workflow, activity
from temporalio.testing import WorkflowEnvironment


# -----------------------------
# ACTIVITY — реальный Temporal
# -----------------------------
@activity.defn
async def say_hello(name: str) -> str:
    print("Activity executed inside Temporal!")
    return f"Hello, {name}!"


# -----------------------------
# WORKFLOW — реальный Temporal
# -----------------------------
@workflow.defn
class HelloWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        await workflow.sleep(1)

        result = await workflow.execute_activity(
            say_hello,
            name,
            schedule_to_close_timeout=timedelta(seconds=5),
        )

        return f"Workflow finished: {result}"


# -----------------------------
# ЛОКАЛЬНЫЙ IN-MEMORY TEMPORAL
# -----------------------------
async def main():
    # реальный Temporal server, но локально в памяти
    async with await WorkflowEnvironment.start_local() as env:
        result = await env.client.execute_workflow(
            HelloWorkflow.run,
            "Ivan",
            id="hello-1",
            task_queue="test-queue",
        )

        print(result)


if __name__ == "__main__":
    asyncio.run(main())