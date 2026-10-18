///
/// @file pattern.hxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief The subscription vocabulary: the text a subscriber names, wildcards included.
/// @version 0.1
/// @date 2026-10-18
/// @copyright Copyright (c) 2026 BA7LYA
///
// SPDX-License-Identifier: MIT

#pragma once

#include <string_view>

namespace ba7lya::msgbus {

///
/// @brief A subscription pattern, as in `site/+/device7/#`: what a subscriber names.
/// @details Borrowed for the same reason `topic` is: naming a pattern is not owning it. A registration
/// parses the text into levels and keeps those levels, so the spelling need only outlive the subscribe
/// call. `+` stands for one level and `#` for the rest; a published topic may use neither.
///
using pattern = std::string_view;

} // namespace ba7lya::msgbus
