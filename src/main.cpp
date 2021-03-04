
#include "cli_args.h"
#include "dpi.h"
#include "oracle_helpers.h"
#include "table.h"

#include "fmt/format.h"
#include "linenoise.h"
#include "tsl/htrie_set.h"
#include "tsl/htrie_map.h"

#include <algorithm>
#include <cctype>
#include <iostream>
#include <iterator>
#include <limits>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>

using namespace sqlplusplus;

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

struct LinenoiseMaskGuard {
    LinenoiseMaskGuard() noexcept {
        linenoiseMaskModeEnable();
    }
    ~LinenoiseMaskGuard() noexcept {
        linenoiseMaskModeDisable();
    }
};

std::function<std::vector<std::string>(std::string_view cmd)> generateCompletions;

bool fetchAndPrintResults(OracleStatement& stmt, int maxResults) {
    if (!stmt.fetch()) {
        std::cout << "No rows returned" << std::endl;
        return false;
    }
    Table table(stmt.numColumns());
    
    table.addRow();
    for (int idx = 1; idx <= stmt.numColumns(); ++idx) {
        auto colInfo = stmt.getColumnInfo(idx);
        table.setColumnValue(0, idx - 1, colInfo.name());
    }

    int resCounter = 0;
    bool moreResults = true;
    while(resCounter < maxResults && moreResults) {
        resCounter++;
        auto rowIdx = table.addRow();
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
            table.setColumnValue(rowIdx, col - 1, colValueStr);
        }
        /*
        for (const auto& col: nulColumns) {
            table.row(resCounter + 1).cell(col).format().font_style({tabulate::FontStyle::italic});
        }*/

        moreResults = stmt.fetch();
    }

    table.render(std::cout);
    std::cout << "Fetched " << resCounter << " rows" << std::endl;
    return moreResults;
}

class Command;
tsl::htrie_map<char, Command*>& getCommandMap() {
    static tsl::htrie_map<char, Command*> globalMap;
    return globalMap;
}

class Command {
public:
    virtual ~Command() = default;

    explicit Command(std::string_view name)
    {
        getCommandMap().insert(name, this);
    }

    virtual std::string_view name() const noexcept = 0;
    virtual bool run(OracleConnection& conn, std::string_view cmdLine) = 0;
};

class DescribeCommand : public Command {
public:
    constexpr static auto kName = std::string_view(".describe");
    DescribeCommand() : Command(kName) {}

    std::string_view name() const noexcept override {
        return kName;
    }

    bool run(OracleConnection& conn, std::string_view tableName) override {
        if (tableName.empty()) {
            throw std::runtime_error("describe command requires a table name");
        }
        OracleConnection::VariableOpts varopts;
        varopts.dbTypeNum = DPI_ORACLE_TYPE_CHAR;
        varopts.nativeTypeNum = DPI_NATIVE_TYPE_BYTES;
        varopts.opts = OracleConnection::VariableOpts::ByteBufferOpts{
            static_cast<uint32_t>(tableName.size()), false};
        varopts.maxArraySize = 1;
        auto var = conn.newArrayVariable(varopts);

        std::string tableNameUpper;
        tableNameUpper.reserve(tableName.size());
        std::transform(
                tableName.begin(),
                tableName.end(),
                std::back_inserter(tableNameUpper),
                [](const auto ch) {
            return std::toupper(ch);
        });

        var.setFrom(0, tableNameUpper);

        constexpr auto describeStmtStr = \
            "select column_name as \"Name\", "
            "nullable as \"Null?\", "
            "concat(concat(concat(data_type,'('),data_length),')') as \"Type\" "
            "from all_tab_columns where table_name = :1";

        auto describeStatement = conn.prepareStatement(describeStmtStr);
        describeStatement.bindByPos(1, var);
        describeStatement.execute();
        fetchAndPrintResults(describeStatement, std::numeric_limits<int>::max());

        return true;
    }
} cmdDescribe;

class ExitCommand : public Command {
public:
    constexpr static auto kName = std::string_view(".exit");
    ExitCommand() : Command(kName) {}

    std::string_view name() const noexcept override {
        return kName;
    }

    bool run(OracleConnection& conn, std::string_view cmdLine) override {
        return false;
    }
} cmdExit;


class MoreRowsCommand : public Command {
public:
    constexpr static auto kName = std::string_view(".moreRows");
    MoreRowsCommand() : Command(kName) {}

    std::string_view name() const noexcept override {
        return kName;
    }

    bool run(OracleConnection& conn, std::string_view cmdLine) override {
        if (!_activeStatement) {
            std::cout << "No active statement" << std::endl;
            return true;
        }

        if (!fetchAndPrintResults(*_activeStatement, 20)) {
            _activeStatement = std::nullopt;
        }
        return true;
    }

    void setActiveStatement(OracleStatement stmt) {
        _activeStatement = std::move(stmt);
    }

private:
    std::optional<OracleStatement> _activeStatement;
} moreRowsCmd;

tsl::htrie_set<char> populateReservedKeywords(OracleConnection& conn) {
    tsl::htrie_set<char> out;
    for (const auto& cmdName: getCommandMap()) {
        out.insert(cmdName->name());
    }

    constexpr static std::string_view selectKeywordsStmtStr
        ("select lower(KEYWORD) from V$RESERVED_WORDS where LENGTH(KEYWORD) > 1");

    auto selectKeywordsStmt = conn.prepareStatement(selectKeywordsStmtStr);
    selectKeywordsStmt.execute();
    while (selectKeywordsStmt.fetch()) {
        auto valueFromDB = selectKeywordsStmt.getColumnValue(1).as<std::string_view>();
        out.insert(valueFromDB);
    }

    return out;
}

int main(int argc, const char** argv) try {
    CliArgumentParser argParser;
    CliArgument connStringArg(argParser, "connectionString", 'c');
    CliArgument usernameArg(argParser, "username", 'u');
    CliArgument passwordarg(argParser, "password", 'p');
    CliArgument historyFileArg(argParser, "historyFile");
    CliArgument historyMaxSizeArg(argParser, "maxHistorySize");
    CliFlag helpFlag(argParser, "help", 'h');

    auto res = argParser.parse(argc, argv);

    if (helpFlag) {
        print_usage(res.program_name);
    }

    std::string historyPath;
    if (historyFileArg) {
        historyPath = historyFileArg.as<std::string>();
        linenoiseHistoryLoad(historyPath.c_str());
    } else if (auto homeVar = ::getenv("HOME"); homeVar != nullptr) {
        historyPath = fmt::format("{}/.sqlplusplus_history", homeVar);
        linenoiseHistoryLoad(historyPath.c_str());
    }

    auto oracleCtx = OracleContext::make();
    OracleConnectionOptions connOpts;
    connOpts.connString = connStringArg.as<std::string>();
    connOpts.username = usernameArg.as<std::string>();
    if (passwordarg) {
        connOpts.password = passwordarg.as<std::string>();
    } else {
        LinenoiseMaskGuard maskGuard;
        auto linenoisePtr = linenoise("Password > ");
        LinenoiseFreeHelper freeHelper(linenoisePtr);
        connOpts.password = std::string(linenoisePtr);
    }

    if (historyMaxSizeArg) {
        linenoiseHistorySetMaxLen(historyMaxSizeArg.as<int64_t>());
    } else {
        // Make history really big by default
        linenoiseHistorySetMaxLen(10000);
    }

    linenoiseSetCompletionCallback([](const char* strPtr, linenoiseCompletions* lc) {
        if (!generateCompletions) {
            return;
        }

        for (const auto& completion : generateCompletions(std::string_view(strPtr))) {
            linenoiseAddCompletion(lc, completion.c_str());
        }
    });

    auto oracleConn = OracleConnection::make(oracleCtx.get(), connOpts);
    auto reservedKeywords = populateReservedKeywords(oracleConn);
    generateCompletions = [&](std::string_view sv) -> std::vector<std::string> {
        std::vector<std::string> ret;

        if (sv.empty()) {
            return ret;
        }

        auto lastWordBoundary = sv.find_last_of(" (),.@");
        if (lastWordBoundary == std::string_view::npos || lastWordBoundary == sv.size() || lastWordBoundary == 0) {
            lastWordBoundary = 0;
        } else {
            ++lastWordBoundary;
        }

        std::string_view lastWord = sv.substr(lastWordBoundary);

        auto prefixRange = reservedKeywords.equal_prefix_range(lastWord);
        for (auto it = prefixRange.first; it != prefixRange.second; ++it) {
            ret.push_back(fmt::format("{}{}", sv.substr(0, lastWordBoundary), it.key()));
        }

        return ret;
    };

    std::optional<OracleStatement> activeStatement;
    std::stringstream lineBuilder;
    bool inMultLine = false;
    for(;;) {
        auto linePtr = linenoise(inMultLine ? "SQL++ (cont.) > " : "SQL++ > ");
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

        if (fullLine.empty()) {
            continue;
        }

        const auto& commandMap = getCommandMap();
        if (auto cmdIt = commandMap.longest_prefix(fullLine); cmdIt != commandMap.end()) {
            auto commandName = cmdIt.value()->name();
            size_t prefixEnd = 0;
            auto checkPrefix = [&] {
                if (prefixEnd == fullLine.size() || prefixEnd == commandName.size()) {
                    return false;
                }
                return fullLine.at(prefixEnd) == commandName.at(prefixEnd);
            };

            for (; checkPrefix(); ++prefixEnd);
            if (prefixEnd < fullLine.size()) {
                auto afterSpaces = fullLine.substr(prefixEnd).find_first_not_of(" ");
                if (afterSpaces != std::string_view::npos) {
                    prefixEnd += afterSpaces;
                }
            }

            if (!cmdIt.value()->run(oracleConn, fullLine.substr(prefixEnd))) {
                break;
            }
            continue;
        }

        try {
            auto activeStatement = oracleConn.prepareStatement(fullLine);
            activeStatement.execute();
            linenoiseHistoryAdd(fullLine.c_str());
            fetchAndPrintResults(activeStatement, 20);
            moreRowsCmd.setActiveStatement(std::move(activeStatement));
        } catch(const OracleException& e) {
            std::cerr << "Error " << e.context() << ": " << e.what() << std::endl;
        }
    }

    if (!historyPath.empty()) {
        linenoiseHistorySave(historyPath.c_str());
    }

    return 0;
} catch(const OracleException& e) {
    std::cerr << "Fatal error " << e.context() << ": " << e.what() << std::endl;
    return 1;
}

