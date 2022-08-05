/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.fnz.logging.structured;

import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.util.StringBuilderFormattable;
import org.apache.logging.log4j.util.StringBuilders;

public class StructuredMessage implements Message, StringBuilderFormattable {

        private static final long serialVersionUID = -5903272448334166185L;

        private transient Object obj;
        private transient String message;

        /**
         * Creates the ObjectMessage.
         *
         * @param obj The Object to format.
         */
        public StructuredMessage(final String message, final Object obj) {
            this.message = message == null ? "null" : message;
            this.obj = obj == null ? "null" : obj;
        }

        /**
         * should return the "format used for the message"
         *
         * @return the formatted object message.
         */
        @Override
        public String getFormattedMessage() {
            StringBuilder sb = new StringBuilder();
            formatTo(sb);
            return sb.toString();
        }
        
        public String getMessage() {
            return message;
        }

        @Override
        public void formatTo(final StringBuilder buffer) {
            buffer.append(message);
            buffer.append(", ");
            StringBuilders.appendValue(buffer, obj);            
        }

        /**
         * Returns the object formatted using its toString method.
         *
         * @return the String representation of the object.
         */
        @Override
        public String getFormat() {
            return getFormattedMessage();
        }

        /**
         * Returns the object parameter.
         *
         * @return The object.
         * @since 2.7
         */
        public Object getParameter() {
            return obj;
        }

        /**
         * Returns the object as if it were a parameter.
         *
         * @return The object.
         */
        @Override
        public Object[] getParameters() {
            return new Object[] {obj};
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final StructuredMessage that = (StructuredMessage) o;
            return obj == null ? that.obj == null : equalObjectsOrStrings(obj, that.obj);
        }

        private boolean equalObjectsOrStrings(final Object left, final Object right) {
            return left.equals(right) || String.valueOf(left).equals(String.valueOf(right));
        }

        @Override
        public int hashCode() {
            return obj != null ? obj.hashCode() : 0;
        }

        @Override
        public String toString() {
            return getFormattedMessage();
        }

        /**
         * Gets the message if it is a throwable.
         *
         * @return the message if it is a throwable.
         */
        @Override
        public Throwable getThrowable() {
            return obj instanceof Throwable ? (Throwable) obj : null;
        }
}
