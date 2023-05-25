import re

from boltons.iterutils import first

from apiserver.apierrors import errors
from apiserver.es_factory import es_factory
from apiserver.tests.automated import TestService
from apiserver.utilities.dicts import nested_get


class TestReports(TestService):
    def _delete_project(self, name):
        existing_project = first(
            self.api.projects.get_all_ex(
                name=f"^{re.escape(name)}$", search_hidden=True
            ).projects
        )
        if existing_project:
            self.api.projects.delete(
                project=existing_project.id, force=True, delete_contents=True
            )

    def test_create_update_move(self):
        task_name = "Rep1"
        comment = "My report"
        tags = ["hello"]

        # report creates a hidden task under hidden .reports subproject
        self._delete_project(".reports")
        task_id = self._temp_report(name=task_name, comment=comment, tags=tags)
        task = self.api.tasks.get_all_ex(id=[task_id]).tasks[0]
        self.assertEqual(task.name, task_name)
        self.assertEqual(task.comment, comment)
        self.assertEqual(set(task.tags), set(tags))
        self.assertEqual(task.type, "report")
        self.assertEqual(set(task.system_tags), {"hidden", "reports"})
        projects = self.api.projects.get_all_ex(name=r"^\.reports$").projects
        self.assertEqual(len(projects), 0)
        project = self.api.projects.get_all_ex(
            name=r"^\.reports$", search_hidden=True
        ).projects[0]
        self.assertEqual(project.id, task.project.id)
        self.assertEqual(set(project.system_tags), {"hidden", "reports"})
        ret = self.api.reports.get_tags()
        self.assertEqual(ret.tags, sorted(tags))

        # update is working on draft reports
        new_comment = "My new comment"
        res = self.api.reports.update(
            task=task_id,
            comment=new_comment,
            tags=[],
            report_assets=["file://test.jpg"],
        )
        self.assertEqual(res.updated, 1)
        task = self.api.tasks.get_all_ex(id=[task_id]).tasks[0]
        self.assertEqual(task.name, task_name)
        self.assertEqual(task.comment, new_comment)
        self.assertEqual(task.tags, [])
        ret = self.api.reports.get_tags()
        self.assertEqual(ret.tags, [])
        self.assertEqual(task.report_assets, ["file://test.jpg"])
        self.api.reports.publish(task=task_id)
        with self.api.raises(errors.bad_request.InvalidTaskStatus):
            self.api.reports.update(task=task_id, report="New report text")

        # update on tags or rename can be done for published report too
        self.api.reports.update(
            task=task_id, name="new name", tags=["test"], comment="Yet another comment"
        )
        task = self.api.tasks.get_all_ex(id=[task_id]).tasks[0]
        self.assertEqual(task.tags, ["test"])
        self.assertEqual(task.name, "new name")
        self.assertEqual(task.comment, "Yet another comment")

        # move under another project autodeletes the empty project
        new_project_name = "Reports Test"
        self._delete_project(new_project_name)
        task2_id = self._temp_report(name="Rep2")
        new_project_id = self.api.reports.move(
            task=task_id, project_name=new_project_name
        ).project_id
        new_project = self.api.projects.get_all_ex(id=[new_project_id]).projects[0]
        self.assertEqual(new_project.name, f"{new_project_name}/.reports")
        self.assertEqual(set(new_project.system_tags), {"hidden", "reports"})
        self.assertEqual(len(self.api.projects.get_all_ex(id=project.id).projects), 1)
        self.api.reports.move(task=task2_id, project=new_project_id)
        self.assertEqual(len(self.api.projects.get_all_ex(id=project.id).projects), 0)
        tasks = self.api.tasks.get_all_ex(
            project=new_project_id, search_hidden=True
        ).tasks
        self.assertTrue({task_id, task2_id}.issubset({t.id for t in tasks}))

        project_id = self.api.reports.move(task=task2_id, project=None).project_id
        project = self.api.projects.get_all_ex(id=[project_id]).projects[0]
        self.assertEqual(project.get("parent"), None)
        self.assertEqual(project.name, ".reports")

    def test_reports_search(self):
        report_task = self._temp_report(name="Rep1")
        non_report_task = self._temp_task(name="hello")
        res = self.api.reports.get_all_ex(
            _any_={"pattern": "hello", "fields": ["name", "id", "tags", "report"]}
        ).tasks
        self.assertEqual(len(res), 0)

        self.api.reports.update(task=report_task, report="hello world")
        res = self.api.reports.get_all_ex(
            _any_={"pattern": "hello", "fields": ["name", "id", "tags", "report"]}
        ).tasks
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0].id, report_task)

    def test_reports_task_data(self):
        report_task = self._temp_report(name="Rep1")
        for model_events in (False, True):
            if model_events:
                non_report_task = self._temp_model(name="hello")
                event_args = {"model_event": True}
            else:
                non_report_task = self._temp_task(name="hello")
                event_args = {}
            debug_image_events = [
                self._create_task_event(
                    task=non_report_task,
                    type_="training_debug_image",
                    iteration=1,
                    metric=f"Metric_{m}",
                    variant=f"Variant_{v}",
                    url=f"{m}_{v}",
                    **event_args,
                )
                for m in range(2)
                for v in range(2)
            ]
            plot_events = [
                self._create_task_event(
                    task=non_report_task,
                    type_="plot",
                    iteration=1,
                    metric=f"Metric_{m}",
                    variant=f"Variant_{v}",
                    plot_str=f"Hello plot",
                    **event_args,
                )
                for m in range(2)
                for v in range(2)
            ]
            self.send_batch([*debug_image_events, *plot_events])

            res = self.api.reports.get_task_data(
                id=[non_report_task], only_fields=["name"], model_events=model_events
            )
            self.assertEqual(len(res.tasks), 1)
            self.assertEqual(res.tasks[0].id, non_report_task)
            self.assertFalse(any(field in res for field in ("plots", "debug_images")))

            res = self.api.reports.get_task_data(
                id=[non_report_task],
                only_fields=["name"],
                debug_images={"metrics": []},
                plots={"metrics": [{"metric": "Metric_1"}]},
                model_events=model_events,
            )
            self.assertEqual(len(res.debug_images), 1)
            task_events = res.debug_images[0]
            self.assertEqual(task_events.task, non_report_task)
            self.assertEqual(len(task_events.iterations), 1)
            self.assertEqual(len(task_events.iterations[0].events), 4)

            self.assertEqual(len(res.plots), 1)
            for m, v in (("Metric_1", "Variant_0"), ("Metric_1", "Variant_1")):
                tasks = nested_get(res.plots, (m, v))
                self.assertEqual(len(tasks), 1)
                task_plots = tasks[non_report_task]
                self.assertEqual(len(task_plots), 1)
                iter_plots = task_plots["1"]
                self.assertEqual(iter_plots.name, "hello")
                self.assertEqual(len(iter_plots.plots), 1)
                ev = iter_plots.plots[0]
                self.assertEqual(ev["metric"], m)
                self.assertEqual(ev["variant"], v)
                self.assertEqual(ev["task"], non_report_task)
                self.assertEqual(ev["iter"], 1)

    @staticmethod
    def _create_task_event(type_, task, iteration, **kwargs):
        return {
            "worker": "test",
            "type": type_,
            "task": task,
            "iter": iteration,
            "timestamp": kwargs.get("timestamp") or es_factory.get_timestamp_millis(),
            **kwargs,
        }

    delete_params = {"force": True}

    def _temp_report(self, name, **kwargs):
        return self.create_temp(
            "reports",
            name=name,
            object_name="task",
            delete_params=self.delete_params,
            **kwargs,
        )

    def _temp_task(self, name, **kwargs):
        return self.create_temp(
            "tasks",
            name=name,
            type="training",
            delete_params=self.delete_params,
            **kwargs,
        )

    def _temp_model(self, name="test model events", **kwargs):
        self.update_missing(
            kwargs, name=name, uri="file:///a/b", labels={}, ready=False
        )
        return self.create_temp("models", delete_params=self.delete_params, **kwargs)

    def send_batch(self, events):
        _, data = self.api.send_batch("events.add_batch", events)
        return data
