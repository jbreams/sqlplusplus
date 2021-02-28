#include "table.h"

#include <iostream>
#include <map>

#include "fmt/format.h"

namespace sqlplusplus {

Table::Table(Width numColumns) :
    columns(numColumns)
{}

Table::RowIndex Table::addRow() {
    auto rowIndex = numRows++;
    values.resize((rowIndex * columns.size()) + columns.size());
    return rowIndex;
}

size_t Table::_resolveValueIdx(RowIndex row, Width column) const {
    if (column >= columns.size()) {
        throw std::runtime_error(
                fmt::format(
                    "column {} is out-of-range. table has {} columns", column, columns.size()));
    }
    if (row >= numRows) {
        throw std::runtime_error(
                fmt::format(
                    "row index {} is out-of-range. table has {} rows", row, numRows));
    }

    return (row * columns.size()) + column;
}

const std::string& Table::columnValue(RowIndex row, Width column) const {
    return values.at(_resolveValueIdx(row, column));
}

void Table::setColumnValue(RowIndex row, Width column, std::string value) {
    if (value.size() > std::numeric_limits<Width>::max()) {
        throw std::runtime_error("table value width over flow");
    }

    auto& strValue = values.at(_resolveValueIdx(row, column));
    strValue = std::move(value);
    auto& colInfo = columns.at(column);
    colInfo.minValueWidth = std::min(colInfo.minValueWidth, static_cast<Width>(strValue.size()));
    colInfo.maxValueWidth = std::max(colInfo.maxValueWidth, static_cast<Width>(strValue.size()));
}

void Table::render(std::ostream& out) {
    if (numRows == 0 || columns.size() == 0) {
        return;
    }

    for (RowIndex rowIndex = 0; rowIndex < numRows; ++rowIndex) {
        const auto& borders = (rowIndex == 0) ? firstRowBorders : otherRowBorders;
        out << borders.left;
        for (Width colIndex = 0; colIndex < columns.size(); ++colIndex) {
            const auto& columnInfo = columns.at(colIndex);
            const auto columnWidth = std::max(columnInfo.configuredWidth, columnInfo.maxValueWidth);
            if (colIndex != 0) {
                out << borders.divider;
            }
            for (Width idx = 0; idx < columnWidth + (padding * 2); ++idx) {
                out << borders.rowBorder;
            }
        }
        out << borders.right << "\n";

        std::map<Width, std::string> remainingFromCurrentRow;
        bool hasIncompleteRows;
        do {
            hasIncompleteRows = false;
            for (Width colIndex = 0; colIndex < columns.size(); ++colIndex) {
                const auto& columnInfo = columns.at(colIndex);
                const auto columnWidth = std::max(columnInfo.configuredWidth, columnInfo.maxValueWidth);
                std::string ownedCurrentWrappedSegment;
                const auto& value = [&]{
                    if (remainingFromCurrentRow.find(colIndex) == remainingFromCurrentRow.end()) {
                        const auto& fullValue = columnValue(rowIndex, colIndex);
                        auto newLineAt = fullValue.find_first_of("\n");
                        if (newLineAt == std::string::npos) {
                            remainingFromCurrentRow[colIndex] = "";
                            return fullValue;
                        }

                        ownedCurrentWrappedSegment = fullValue.substr(0, newLineAt);
                        remainingFromCurrentRow.insert({colIndex, fullValue.substr(newLineAt + 1)});
                        hasIncompleteRows = true;
                    } else {
                        const auto& remaining = remainingFromCurrentRow[colIndex];
                        auto newlineAt = remaining.find_first_of("\n");
                        if (newlineAt != std::string::npos) {
                            ownedCurrentWrappedSegment = remaining.substr(0, newlineAt);
                            remainingFromCurrentRow[colIndex] = remaining.substr(newlineAt + 1);
                            hasIncompleteRows = true;
                        } else {
                            ownedCurrentWrappedSegment = remaining;
                            remainingFromCurrentRow[colIndex] = "";
                        }
                    }
                    
                    return ownedCurrentWrappedSegment;
                }();
                fmt::print("{0}{1: >{2}}{3}{1: >{4}}",
                        borders.cellBorder, "", padding, value, (columnWidth - value.size()) + padding);
            }
            out << borders.cellBorder << "\n";
        } while(hasIncompleteRows);
    }
   
    out << lastRowBorders.left;
    for (Width colIndex = 0; colIndex < columns.size(); ++colIndex) {
        if (colIndex != 0) {
            std::cout << lastRowBorders.divider;
        }
        const auto& columnInfo = columns.at(colIndex);
        const auto columnWidth = std::max(columnInfo.configuredWidth, columnInfo.maxValueWidth);
        for (Width idx = 0; idx < columnWidth + (padding * 2); ++idx) {
            out << lastRowBorders.rowBorder;
        }
    }
    out << lastRowBorders.right << std::endl;
}

} // namespace sqlplusplus
