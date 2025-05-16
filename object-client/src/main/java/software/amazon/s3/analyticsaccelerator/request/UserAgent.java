/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package software.amazon.s3.analyticsaccelerator.request;

import java.util.Objects;
import lombok.Getter;

/** User-Agent to be used by ObjectClients */
@Getter
public final class UserAgent {
  // Hard-coded user-agent string
  private static final String UA_STRING = "s3analyticsaccelerator";

  private static final String VERSION_INFO = "1.0.0";
  /**
   * Disallowed characters in the user agent token: @see <a
   * href="https://tools.ietf.org/html/rfc7230#section-3.2.6">RFC 7230</a>
   */
  private static final String UA_DENYLIST_REGEX = "[() ,/:;<=>?@\\[\\]{}\\\\]";

  private String userAgent = UA_STRING + "/" + VERSION_INFO + "/" + "sharedCache";

  /**
   * Prepend hard-coded user-agent string with input string provided.
   *
   * @param userAgentPrefix to prepend the default user-agent string
   */
  public void prepend(String userAgentPrefix) {
    if (Objects.nonNull(userAgentPrefix))
      this.userAgent = sanitizeInput(userAgentPrefix) + " " + this.userAgent;
  }

  private static String sanitizeInput(String input) {
    return input == null ? "unknown" : input.replaceAll(UA_DENYLIST_REGEX, "_");
  }
}
