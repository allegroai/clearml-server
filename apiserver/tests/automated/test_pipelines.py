from typing import Tuple

from apiserver.apierrors import errors
from apiserver.tests.automated import TestService


class TestPipelines(TestService):
    def test_delete_runs(self):
        queue = self.api.queues.get_default().id
        task_name = "pipelines test"
        project, task = self._temp_project_and_task(name=task_name)
        args = [{"name": "hello", "value": "test"}]
        pipeline_tasks = [
            self.api.pipelines.start_pipeline(
                task=task, queue=queue, args=args
            ).pipeline
            for _ in range(2)
        ]
        tasks = self.api.tasks.get_all_ex(project=project).tasks
        self.assertEqual({task, *pipeline_tasks}, {t.id for t in tasks})

        # cannot delete all runs
        with self.api.raises(errors.bad_request.CannotRemoveAllRuns):
            self.api.pipelines.delete_runs(project=project, ids=[task, *pipeline_tasks])

        # successful deletion
        res = self.api.pipelines.delete_runs(project=project, ids=pipeline_tasks)
        self.assertEqual({r.id for r in res.succeeded}, set(pipeline_tasks))
        tasks = self.api.tasks.get_all_ex(project=project).tasks
        self.assertEqual([task], [t.id for t in tasks])

    def test_start_pipeline(self):
        queue = self.api.queues.get_default().id
        task_name = "pipelines test"
        project, task = self._temp_project_and_task(name=task_name)
        args = [{"name": "hello", "value": "test"}]

        res = self.api.pipelines.start_pipeline(task=task, queue=queue, args=args)
        pipeline_task = res.pipeline
        try:
            self.assertTrue(res.enqueued)
            pipeline = self.api.tasks.get_all_ex(id=[pipeline_task]).tasks[0]
            self.assertTrue(pipeline.name.startswith(task_name))
            self.assertEqual(pipeline.status, "queued")
            self.assertEqual(pipeline.project.id, project)
            self.assertEqual(
                pipeline.hyperparams.Args,
                {
                    a["name"]: {
                        "section": "Args",
                        "name": a["name"],
                        "value": a["value"],
                    }
                    for a in args
                },
            )
        finally:
            self.api.tasks.delete(task=pipeline_task, force=True)

    def _temp_project_and_task(self, name) -> Tuple[str, str]:
        project = self.create_temp(
            "projects", name=name, description="test", delete_params=dict(force=True),
        )

        return (
            project,
            self.create_temp(
                "tasks",
                name=name,
                type="controller",
                project=project,
                system_tags=["pipeline"],
            ),
        )
