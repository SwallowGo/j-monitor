package org.swallow.record;

public record PushPlusToRequest(String token,
                                String title,
                                String content,
                                String to,
                                String template) {
}
