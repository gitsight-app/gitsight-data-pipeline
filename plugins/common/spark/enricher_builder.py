from common.spark.spark_enrichers import BaseEnricher


class SparkConfigBuilder:
    def __init__(self):
        self._conf = {}

    def apply(self, enricher: BaseEnricher | list[BaseEnricher]):
        if isinstance(enricher, list):
            for e in enricher:
                self.apply(e)
            return self

        conf_fragment = enricher.build()
        if conf_fragment:
            self._conf.update(conf_fragment)
        return self

    def build(self) -> dict:
        return self._conf.copy()
