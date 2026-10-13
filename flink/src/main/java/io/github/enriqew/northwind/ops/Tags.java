package io.github.enriqew.northwind.ops;

import io.github.enriqew.northwind.model.Disposition;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.OutputTag;

/** Side outputs shared by the operators. */
public final class Tags {
    private Tags() {}

    /** One disposition per input message, from whichever operator decided its fate. */
    public static final OutputTag<Disposition> AUDIT =
            new OutputTag<>("audit", TypeInformation.of(Disposition.class));

    /** Late and invalid messages, kept individually for the lateness and quality analysis. */
    public static final OutputTag<Disposition> REJECTED =
            new OutputTag<>("rejected", TypeInformation.of(Disposition.class));
}
