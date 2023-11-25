#include "cache.hh"
#include "fetchers.hh"
#include "store-api.hh"

#include "toml11/toml/parser.hpp"

#include <nlohmann/json.hpp>
#include <chrono>

using namespace std::string_literals;
using namespace std::string_view_literals;

namespace nix::fetchers {

struct PijulInputScheme : InputScheme
{
    [[nodiscard]]
    std::optional<Input> inputFromURL(const ParsedURL &url, bool requireTree) const override
    {
        if (url.scheme != "pijul+http" && url.scheme != "pijul+https" && url.scheme != "pijul+ssh") {
            return {};
        }

        auto url2(url);
        url2.scheme = std::string(url2.scheme, 6);
        url2.query.clear();

        Attrs attrs;
        attrs.emplace("type"s, "pijul"s);

        for (const auto &[name, value]: url.query) {
            if (name == "channel" || name == "state") {
                attrs.emplace(name, value);
            } else {
                url2.query.emplace(name, value);
            }
        }

        attrs.emplace("url"s, url2.to_string());

        return inputFromAttrs(attrs);
    }

    [[nodiscard]]
    std::optional<Input> inputFromAttrs(const Attrs &attrs) const override
    {
        if (maybeGetStrAttr(attrs, "type") != "pijul") {
            return {};
        }

        for (const auto &[name, _]: attrs) {
            if (
                name != "type"sv &&
                name != "url"sv && name != "channel"sv && name != "state"sv &&
                name != "narHash"sv && name != "lastModified"sv
            ) {
                throw Error("unsupported Pijul input attribute '%s'"s, name);
            }
        }

        parseURL(getStrAttr(attrs, "url"));

        Input input;
        input.attrs = attrs;
        return input;
    }

    [[nodiscard]]
    bool hasAllInfo(const Input &input) const override
    {
        return true;
    }

    [[nodiscard]]
    ParsedURL toURL(const Input &input) const override
    {
        auto url = parseURL(getStrAttr(input.attrs, "url"));

        if (url.scheme != "pijul") {
            url.scheme = "pijul+"s + url.scheme;
        }

        if (auto channel = maybeGetStrAttr(input.attrs, "channel"s)) {
            url.query.insert_or_assign("channel"s, std::move(*channel));
        }

        if (auto state = maybeGetStrAttr(input.attrs, "state"s)) {
            url.query.insert_or_assign("state"s, std::move(*state));
        }

        return url;
    }

    std::pair<StorePath, Input> fetch(
        ref<Store> store,
        const Input &_input
    ) override
    {
        Input input(_input);

        const auto &name = input.getName();

        const Path tmpDir = createTempDir();
        const AutoDelete delTmpDir(tmpDir, true);
        const auto repoDir = tmpDir + "/source"sv;

        const auto url = parseURL(getStrAttr(input.attrs, "url"));
        const auto &repoUrl = url.base;
        const auto channel = maybeGetStrAttr(input.attrs, "channel");
        const auto state = maybeGetStrAttr(input.attrs, "state");

        const Attrs impureKey {
            { "name", name },
            { "url", repoUrl },
        };

        std::optional<Attrs> key;
        bool isLocked = false;

        if (channel && state) {
            isLocked = true;

            key = {
                { "name", input.getName() },
                { "channel", *channel },
                { "state", *state },
            };

            if (auto res = getCache()->lookup(store, *key)) {
                auto &[infoAttrs, storePath] = *res;
                input.attrs.insert_or_assign("lastModified", getIntAttr(infoAttrs, "lastModified"));
                return { std::move(storePath), input };
            }
        }

        if (auto res = getCache()->lookup(store, impureKey)) {
            auto &[infoAttrs, storePath] = *res;
            input.attrs.insert_or_assign("lastModified", getIntAttr(infoAttrs, "lastModified"));

            if ((!channel || *channel == getStrAttr(infoAttrs, "channel")) && (!state || *state == getStrAttr(infoAttrs, "state"))) {
                return { std::move(storePath), std::move(infoAttrs) };
            }
        }

        Strings args { "clone"s };

        if (channel) {
            args.push_back("--channel"s);
            args.push_back(*channel);
        }

        if (state) {
            args.push_back("--state"s);
            args.push_back(*state);
        }

        args.push_back(repoUrl);
        args.push_back(repoDir);

        runProgram("pijul"s, true, args, {}, true);

        const RepoStatus rs = getRepoStatus(repoDir).value();

        if (channel && *channel != rs.channel) {
            throw Error("channel mismatch: requested %s, got %s"s, *channel, rs.channel);
        }

        if (state && *state != rs.state) {
            throw Error("state mismatch: requested %s, got %s"s, *state, rs.state);
        }

        if (!key) {
            key = {
                { "name", input.getName() },
                { "channel", rs.channel },
                { "state", rs.state },
            };
        }

        Attrs infoAttrs {
            { "lastModified", rs.lastModified },
            { "channel", rs.channel },
            { "state", rs.state },
        };
        deletePath(repoDir + "/.pijul"sv);

        auto storePath = store->addToStore(input.getName(), repoDir);

        if (!isLocked) {
            getCache()->add(store, impureKey, infoAttrs, storePath, false);
        }

        getCache()->add(store, *key, infoAttrs, storePath, true);

        input.attrs.merge(infoAttrs);

        return { std::move(storePath), input };
    }

private:
    struct RepoStatus
    {
        std::string channel;
        std::string state;
        uint64_t lastModified;
    };

    static std::optional<RepoStatus> getRepoStatus(const PathView &repoPath)
    {
        auto sp = getState(repoPath);
        auto channel = getRepoChannel(repoPath);

        if (!sp || !channel) {
            return {};
        }

        auto &[state, lastModified] = *sp;

        return RepoStatus {
            .channel = std::move(*channel),
            .state = std::move(state),
            .lastModified = lastModified,
        };
    }

    static std::optional<std::pair<std::string, uint64_t>> getState(const PathView &repoPath)
    {
        const auto &[status, output] = runProgram(RunOptions {
            .program = "pijul",
            .args = { "log", "--output-format", "json", "--state", "--limit", "1" },
            .chdir = Path(repoPath),
        });

        if (status != 0) {
            return {};
        }

        const auto &json = nlohmann::json::parse(output);
        const auto &commitInfo = json.at(0);

        const auto &timestampSpec = commitInfo.at("timestamp").get<std::string>();
        const uint64_t timestamp = std::chrono::duration_cast<std::chrono::seconds>(parseRFC3339(timestampSpec).value().time_since_epoch()).count();

        const std::string &state = commitInfo.at("state");

        return std::make_pair(state, timestamp);
    }

    // FIXME: This is a massive hack, use std::chrono::parse instead
    static std::optional<std::chrono::system_clock::time_point> parseRFC3339(const std::string &spec)
    {
        toml::detail::location loc { "pijul"s, spec };
        // TODO: correct time zone handling???
        auto [res, _] = toml::detail::parse_offset_datetime(loc).unwrap();
        return res;
    }

    static std::optional<std::string> getRepoChannel(const PathView &repoPath)
    {
        const auto &[status, output] = runProgram(RunOptions {
            .program = "pijul",
            .args = { "channel" },
            .chdir = Path(repoPath),
        });

        if (status != 0) {
            return {};
        }

        std::string::size_type pos = 0;

        do {
            const auto nl = output.find('\n', pos);
            const auto line = std::string_view(output).substr(pos, nl - pos);

            if (line.empty()) {
                continue;
            }

            if (line.at(0) == '*') {
                return std::string(line.substr(2));
            }

            pos = nl;
        } while (pos != std::string::npos);

        return {};
    }
};

static auto rPijulInputScheme = OnStartup([] {
    registerInputScheme(std::make_unique<PijulInputScheme>());
});

} // namespace nix::fetchers
