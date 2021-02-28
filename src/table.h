#pragma once

#include <string>
#include <vector>

namespace sqlplusplus {

class Table {
public:
    using Width = uint32_t;
    using RowIndex = uint32_t;
    struct Column {
        Width minValueWidth = 0;
        Width maxValueWidth = 0;
        Width configuredWidth = 0;
    };

    explicit Table(Width numColumns);

    RowIndex addRow();
    const std::string& columnValue(RowIndex row, Width column) const;
    void setColumnValue(RowIndex row, Width column, std::string value);
    void setColumnValue(RowIndex row, Width column, std::string_view value) {
        setColumnValue(row, column, std::string(value));
    }

    std::vector<std::string> values;
    std::vector<Column> columns;
    RowIndex numRows = 0;

    struct CellBorder {
        std::string left;
        std::string divider;
        std::string right;
        std::string rowBorder = "─";
        std::string cellBorder = "│";
    };

    CellBorder firstRowBorders = { "┌", "┬", "┐" };
    CellBorder otherRowBorders = { "├", "┼", "┤" };
    CellBorder lastRowBorders = { "└", "┴", "┘" };
    Width padding = 1;

    void render(std::ostream& out);

private:
    size_t _resolveValueIdx(RowIndex row, Width column) const;
};
} // namespace sqlplusplus
