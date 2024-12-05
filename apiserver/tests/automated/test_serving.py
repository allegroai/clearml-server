from time import time, sleep

from apiserver.apierrors import errors
from apiserver.tests.automated import TestService


class TestServing(TestService):
    def test_status_report(self):
        container_id1 = "container_1"
        container_id2 = "container_2"
        url = "http://test_url"
        container_infos = [
            {
                "container_id": container_id,  # required
                "endpoint_name": "my endpoint",  # required
                "endpoint_url": url,  # can be omitted for register but required for status report
                "model_name": "my model",  # required
                "model_source": "s3//my_bucket",  # optional right now
                "model_version": "3.1.0",  # optional right now
                "preprocess_artifact": "some string here",  # optional right now
                "input_type": "another string here",  # optional right now
                "input_size": 9_000_000,  # optional right now, bytes
                "tags": ["tag1", "tag2"],  # optional
                "system_tags": None,  # optional
            }
            for container_id in (container_id1, container_id2)
        ]

        for container_info in container_infos:
            self.api.serving.register_container(
                **container_info,
                timeout=100,  # expiration timeout in seconds. Optional, the default value is 600
            )
        for idx, container_info in enumerate(container_infos):
            mul = idx + 1
            self.api.serving.container_status_report(
                **container_info,
                uptime_sec=1000 * mul,
                requests_num=1000 * mul,
                requests_min=5 * mul,  # requests per minute
                latency_ms=100 * mul,  # average latency
                machine_stats={   # the same structure here as used by worker status_reports
                    "cpu_usage": [10, 20],
                    "memory_used": 50,
                }
            )
        endpoints = self.api.serving.get_endpoints().endpoints
        details = self.api.serving.get_endpoint_details(endpoint_url=url)
        details = self.api.serving.get_endpoint_details(endpoint_url=url)

        sleep(5)  # give time to ES to accomodate data
        to_date = int(time()) + 40
        from_date = to_date - 100
        res1 = self.api.serving.get_endpoint_metrics_history(
            endpoint_url=url,
            from_date=from_date,
            to_date=to_date,
            interval=1,
        )
        res2 = self.api.serving.get_endpoint_metrics_history(
            endpoint_url=url,
            from_date=from_date,
            to_date=to_date,
            interval=1,
            metric_type="requests_min",
        )
        res3 = self.api.serving.get_endpoint_metrics_history(
            endpoint_url=url,
            from_date=from_date,
            to_date=to_date,
            interval=1,
            metric_type="latency_ms",
        )
        res4 = self.api.serving.get_endpoint_metrics_history(
            endpoint_url=url,
            from_date=from_date,
            to_date=to_date,
            interval=1,
            metric_type="cpu_count",
        )
        res5 = self.api.serving.get_endpoint_metrics_history(
            endpoint_url=url,
            from_date=from_date,
            to_date=to_date,
            interval=1,
            metric_type="cpu_util",
        )
        res6 = self.api.serving.get_endpoint_metrics_history(
            endpoint_url=url,
            from_date=from_date,
            to_date=to_date,
            interval=1,
            metric_type="ram_total",
        )

        for container_id in (container_id1, container_id2):
            self.api.serving.unregister_container(container_id=container_id)
        endpoints = self.api.serving.get_endpoints().endpoints
        with self.api.raises(errors.bad_request.NoContainersForUrl):
            details = self.api.serving.get_endpoint_details(endpoint_url=url)
        pass
