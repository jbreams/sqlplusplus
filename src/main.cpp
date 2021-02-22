
#include "cli_args.h"
#include "dpi.h"
#include "oracle_helpers.h"

#include "fmt/format.h"
#include "linenoise.h"
#include "tabulate/font_style.hpp"
#include "tabulate/table.hpp"

#include <iostream>
#include <limits>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>

using namespace sqlsqrt;

void print_usage(std::string_view program_name)
{
    std::cout << "Synopsis: " << program_name << "[OPTIONS]"
              << "\n"
                 "Options:\n"
                 "  -h, --help               Display command-line synopsis followed by the list of\n"
                 "                           available options.\n"
                 "  -c, --connectionString   Connection string to connect to oracle with\n"
                 "  -u, --username           Username to authenticate to Oracle with\n"
                 "  -p, --password           Password to authenticate to Oracle with\n"
              << std::endl;
}

template <typename T>
struct LinenoiseFreeHelper {
    ~LinenoiseFreeHelper() noexcept {
        ::linenoiseFree(reinterpret_cast<void*>(const_cast<T*>(ptr)));
    }

    explicit LinenoiseFreeHelper(const T* ptr) : ptr(ptr) {}

    const T* ptr;
};

bool fetchAndPrintResults(OracleStatement& stmt, int maxResults) {
    if (!stmt.fetch()) {
        std::cout << "No rows returned" << std::endl;
        return false;
    }
    tabulate::Table table;
    
    std::vector<size_t> columnWidths;
    std::vector<std::variant<std::string, const char*, tabulate::Table>> columnNames;
    for (int idx = 1; idx <= stmt.numColumns(); ++idx) {
        auto colInfo = stmt.getColumnInfo(idx);
        columnNames.push_back(std::string{colInfo.name()});
        columnWidths.push_back(colInfo.name().size());
    }
    table.add_row(columnNames);
    table.row(0).format().font_style({tabulate::FontStyle::bold});

    int resCounter = 0;
    bool wasExhaused = false;
    while(resCounter < maxResults) {
        std::vector<std::variant<std::string, const char*, tabulate::Table>> rowValues;
        std::set<int> nulColumns;
        for (auto col = 1; col <= stmt.numColumns(); ++col) {
            auto colValue = stmt.getColumnValue(col);
            std::string colValueStr;
            if (colValue.isNull()) {
                nulColumns.insert(col - 1);
                colValueStr = "<null>";
            }
            switch(colValue.nativeType()) {
            case DPI_NATIVE_TYPE_BOOLEAN:
                colValueStr = colValue.as<bool>() ? "TRUE" : "FALSE";
                break;
            case DPI_NATIVE_TYPE_BYTES:
                colValueStr = fmt::format("\"{}\"", colValue.as<std::string_view>());
                break;
            case DPI_NATIVE_TYPE_DOUBLE:
                colValueStr = fmt::format("{}", colValue.as<double>());
                break;
            case DPI_NATIVE_TYPE_INT64:
                colValueStr = fmt::format("{}", colValue.as<int64_t>());
                break;
            case DPI_NATIVE_TYPE_UINT64:
                colValueStr = fmt::format("{}", colValue.as<uint64_t>());
                break;
            case DPI_NATIVE_TYPE_FLOAT:
                colValueStr = fmt::format("{}", colValue.as<float>());
                break;
            case DPI_NATIVE_TYPE_TIMESTAMP: {
                auto ts = colValue.as<dpiTimestamp*>();
                colValueStr =fmt::format("{}-{}-{} {}:{}:{}.{} Z{}",
                            ts->year,
                            ts->month,
                            ts->day,
                            ts->hour,
                            ts->minute,
                            ts->second,
                            ts->fsecond,
                            ts->tzHourOffset);
                break;
                                            }
            default:
                colValueStr = "unsupported type";
            }
            columnWidths[col - 1] = std::max(columnWidths[col - 1], colValueStr.size());
            rowValues.push_back(colValueStr);
        }
        table.add_row(rowValues);
        for (const auto& col: nulColumns) {
            table.row(resCounter + 1).cell(col).format().font_style({tabulate::FontStyle::italic});
        }

        if (!stmt.fetch()) {
            wasExhaused = true;
            break;
        }
        resCounter++;
    }
    std::cout << table << "\nFetched " << resCounter << " rows" << std::endl;
    return wasExhaused;
}

void describeTable(OracleConnection& conn, std::string_view tableName) {
    OracleConnection::VariableOpts varopts;
    varopts.dbTypeNum = DPI_ORACLE_TYPE_CHAR;
    varopts.nativeTypeNum = DPI_NATIVE_TYPE_BYTES;
    varopts.opts = OracleConnection::VariableOpts::ByteBufferOpts{
        static_cast<uint32_t>(tableName.size()), false};
    varopts.maxArraySize = 1;
    auto var = conn.newArrayVariable(varopts);
    var.setFrom(0, tableName);

    constexpr auto describeStmtStr = \
        "select column_name as \"Name\", "
        "nullable as \"Null?\", "
        "concat(concat(concat(data_type,'('),data_length),')') as \"Type\" "
        "from all_tab_columns where table_name = :1";

    auto describeStatement = conn.prepareStatement(describeStmtStr);
    describeStatement.bindByPos(1, var);
    describeStatement.execute();
    fetchAndPrintResults(describeStatement, std::numeric_limits<int>::max());
}

constexpr static std::string_view kDescribeKeyword(".describe ");

int main(int argc, const char** argv) try {
    CliArgumentParser argParser;
    CliArgument connStringArg(argParser, "connectionString", 'c');
    CliArgument usernameArg(argParser, "username", 'u');
    CliArgument passwordarg(argParser, "password", 'p');
    CliFlag helpFlag(argParser, "help", 'h');

    auto res = argParser.parse(argc, argv);

    if (helpFlag) {
        print_usage(res.program_name);
    }

    auto oracleCtx = OracleContext::make();
    OracleConnectionOptions connOpts;
    connOpts.connString = connStringArg.as<std::string>();
    connOpts.username = usernameArg.as<std::string>();
    if (passwordarg) {
        connOpts.password = passwordarg.as<std::string>();
    } else {
        linenoiseMaskModeEnable();
        auto linenoisePtr = linenoise("Password > ");
        LinenoiseFreeHelper freeHelper(linenoisePtr);
        connOpts.password = std::string(linenoisePtr);
        linenoiseMaskModeDisable();
    }

    auto oracleConn = OracleConnection::make(oracleCtx.get(), connOpts);
    std::optional<OracleStatement> activeStatement;
    std::stringstream lineBuilder;
    bool inMultLine = false;
    for(;;) {
        auto linePtr = linenoise(inMultLine ? "SQLsqrt (cont.) > " : "SQLsqrt > ");
        if (linePtr == nullptr) {
            break;
        }
        LinenoiseFreeHelper helper(linePtr);
        std::string_view line(linePtr);

        if (line.back() == '\\') {
            lineBuilder << line.substr(0, line.size() - 1);
            linenoiseSetMultiLine(1);
            inMultLine = true;
            continue;
        } else {
            lineBuilder << line;
            inMultLine = false;
            linenoiseSetMultiLine(0);
        }

        auto fullLine = lineBuilder.str();
        lineBuilder = std::stringstream{};

        if (fullLine == ".exit") {
            break;
        } else if (fullLine == ".it") {
            if (!activeStatement) {
                std::cout << "No active statement" << std::endl;
                continue;
            }

            if (!fetchAndPrintResults(*activeStatement, 20)) {
                activeStatement = std::nullopt;
            }
            continue;
        } else if (fullLine.find(kDescribeKeyword) == 0) {
            auto table_name = fullLine.substr(kDescribeKeyword.size());
            describeTable(oracleConn, table_name);
            continue;
        }

        try {
            activeStatement = oracleConn.prepareStatement(fullLine);
            activeStatement->execute();
            linenoiseHistoryAdd(fullLine.c_str());
            fetchAndPrintResults(*activeStatement, 20);

        } catch(const OracleException& e) {
            std::cerr << "Error " << e.context() << ": " << e.what() << std::endl;
        }
    }

    return 0;
} catch(const OracleException& e) {
    std::cerr << "Fatal error " << e.context() << ": " << e.what() << std::endl;
    return 1;
}

