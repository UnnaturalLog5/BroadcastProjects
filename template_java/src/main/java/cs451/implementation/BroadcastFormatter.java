package cs451.implementation;

import java.util.logging.Formatter;
import java.util.logging.LogRecord;


/**
 * Logging format for the BroadcastLogging. Currently deprecated, as the logger was not working properly
 * Replaced with buffered writing to a file.
 */
public class BroadcastFormatter extends Formatter {
    @Override
    public String format(LogRecord record) {
        return record.getMessage() + '\n';
    }
}
