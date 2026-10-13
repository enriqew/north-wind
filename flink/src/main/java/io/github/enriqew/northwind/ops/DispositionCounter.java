package io.github.enriqew.northwind.ops;

import io.github.enriqew.northwind.model.Disposition;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Counts dispositions per category, reason and receiver network, and writes each total once,
 * when the input ends. The conservation check reads these totals.
 */
public class DispositionCounter extends KeyedProcessFunction<String, Disposition, String> {
    private transient ValueState<Long> count;

    @Override
    public void open(Configuration parameters) {
        count = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Types.LONG));
    }

    @Override
    public void processElement(Disposition d, Context ctx, Collector<String> out) throws Exception {
        Long c = count.value();
        if (c == null) {
            c = 0L;
            ctx.timerService().registerEventTimeTimer(Long.MAX_VALUE);
        }
        count.update(c + 1);
    }

    @Override
    public void onTimer(long t, OnTimerContext ctx, Collector<String> out) throws Exception {
        String[] k = ctx.getCurrentKey().split("\\|", -1);
        out.collect(String.format(
                "{\"category\":\"%s\",\"reason\":\"%s\",\"stream\":\"%s\",\"count\":%d}",
                k[0], k[1], k[2], count.value()));
    }
}
