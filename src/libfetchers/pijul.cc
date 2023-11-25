#include "cache.hh"
#include "fetchers.hh"
#include "store-api.hh"

#include "toml11/toml/parser.hpp"

#include <nlohmann/json.hpp>
#include <chrono>

using namespace std::string_literals;
using namespace std::string_view_literals;

namespace nix::fetchers {

std::string runPijul(
    Strings args,
    std::optional<Path> chdir = {},
    std::optional<std::string> input = {},
    bool isInteractive = false
)
{
    auto program = "pijul"sv;

    auto res = runProgram(RunOptions {
        .program = std::string(program),
        .searchPath = true,
        .args = std::move(args),
        .chdir = std::move(chdir),
        .input = std::move(input),
        .isInteractive = isInteractive,
    });

    if (!statusOk(res.first))
        throw ExecError(res.first, "program '%1%' %2%", program, statusToString(res.first));

    return res.second;
}


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

        if (maybeGetStrAttr(input.attrs, "channel") && maybeGetStrAttr(input.attrs, "state")) {
            input.locked = true;
        }

        return input;
    }

    [[nodiscard]]
    bool hasAllInfo(const Input &input) const override
    {
        return maybeGetIntAttr(input.attrs, "lastModified").has_value();
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
        auto [storePath, infoAttrs] = doFetch(store, _input);

        Input input(_input);
        mergeAttrs(input.attrs, std::move(infoAttrs));
        return { std::move(storePath), input };
    }

    std::optional<Path> getSourcePath(const Input &input) override
    {
        auto url = parseURL(getStrAttr(input.attrs, "url"));

        if (url.scheme == "file" && !input.getRef() && !input.getRev()) {
            return url.path;
        }

        return {};
    }

    void markChangedFile(const Input &input, std::string_view file, std::optional<std::string> commitMsg) override
    {
        auto sourcePath = getSourcePath(input);
        assert(sourcePath);

        runPijul({ "add", "--", std::string(file) }, sourcePath);

        if (commitMsg)
            runPijul({ "record", std::string(file), "-m", *commitMsg }, sourcePath, {}, true);
    }

private:
    struct RepoStatus
    {
        std::string channel;
        std::string state;
        uint64_t lastModified;
    };

    static std::pair<StorePath, Attrs> doFetch(const ref<Store> &store, const Input &input)
    {
        const auto &name = input.getName();

        const auto url = parseURL(getStrAttr(input.attrs, "url"));
        const auto &repoUrl = url.base;
        const auto channel = maybeGetStrAttr(input.attrs, "channel");
        const auto state = maybeGetStrAttr(input.attrs, "state");

        const Attrs impureKey {
            { "type", "pijul" },
            { "name", name },
            { "url", repoUrl },
        };

        std::optional<Attrs> key;
        bool isLocked = false;

        if (channel && state) {
            isLocked = true;

            key = {
                { "type", "pijul" },
                { "name", name },
                { "channel", *channel },
                { "state", *state },
            };

            if (auto res = getCache()->lookup(store, *key)) {
                auto &[infoAttrs, storePath] = *res;
                return { std::move(storePath), std::move(infoAttrs) };
            }
        }

        if (auto res = getCache()->lookup(store, impureKey)) {
            auto &[infoAttrs, storePath] = *res;

            if ((!channel || *channel == getStrAttr(infoAttrs, "channel")) && (!state || *state == getStrAttr(infoAttrs, "state"))) {
                return { std::move(storePath), std::move(infoAttrs) };
            }
        }

        auto [storePath, rs] = doFetch(store, name, repoUrl, channel, state);

        if (!key) {
            key = {
                { "type", "pijul" },
                { "name", name },
            };
        }

        mergeAttrs(*key, {
            { "channel", rs.channel },
            { "state", rs.state },
        });

        Attrs infoAttrs = {
            { "channel", std::move(rs.channel) },
            { "state", std::move(rs.state) },
            { "lastModified", rs.lastModified },
        };

        if (!isLocked) {
            getCache()->add(store, impureKey, infoAttrs, storePath, false);
        }

        getCache()->add(store, *key, infoAttrs, storePath, true);

        return { std::move(storePath), std::move(infoAttrs) };
    }

    static std::pair<StorePath, RepoStatus> doFetch(
        const ref<Store> &store,
        const std::string_view &inputName,
        const std::string_view &repoUrl,
        const std::optional<std::string_view> &channel,
        const std::optional<std::string_view> &state
    )
    {
        const Path tmpDir = createTempDir();
        const AutoDelete delTmpDir(tmpDir, true);
        const auto repoDir = tmpDir + "/source"sv;

        Strings args { "clone"s };

        if (channel) {
            args.push_back("--channel"s);
            args.emplace_back(*channel);
        }

        if (state) {
            args.push_back("--state"s);
            args.emplace_back(*state);
        }

        args.emplace_back(repoUrl);
        args.push_back(repoDir);

        runPijul(args, {}, {}, true);

        RepoStatus rs = getRepoStatus(repoDir);

        if (channel && *channel != rs.channel) {
            throw Error("channel mismatch: requested %s, got %s"s, *channel, rs.channel);
        }

        if (state && *state != rs.state) {
            throw Error("state mismatch: requested %s, got %s"s, *state, rs.state);
        }

        deletePath(repoDir + "/.pijul"sv);

        auto storePath = store->addToStore(inputName, repoDir);

        return { std::move(storePath), std::move(rs) };
    }

    static void mergeAttrs(Attrs &dest, Attrs &&source)
    {
        while (true) {
            auto next = source.begin();

            if (next == source.end()) {
                break;
            }

            auto handle = source.extract(next);

            mergeOne(dest, std::move(handle.key()), std::move(handle.mapped()));
        }
    }

    static void mergeOne(Attrs &dest, std::string key, Attr attr)
    {
        const auto &d = dest.find(key);

        if (d != dest.end()) {
            if (d->second != attr) {
                throw Error("while merging attrs: value mismatch for %s", d->first);
            }
        } else {
            dest.emplace(std::move(key), std::move(attr));
        }
    }

    static RepoStatus getRepoStatus(const PathView &repoPath)
    {
        auto [state, lastModified] = getState(repoPath);
        auto channel = getRepoChannel(repoPath);

        return RepoStatus {
            .channel = std::move(channel),
            .state = std::move(state),
            .lastModified = lastModified,
        };
    }

    static std::pair<std::string, uint64_t> getState(const PathView &repoPath)
    {
        const auto &output = runPijul(
            { "log", "--output-format", "json", "--state", "--limit", "1" },
            Path(repoPath)
        );

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

    static std::string getRepoChannel(const PathView &repoPath)
    {
        const auto &output = runPijul({ "channel" }, Path(repoPath));

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

        throw Error("could not parse current channel"s);
    }
};

static auto rPijulInputScheme = OnStartup([] {
    registerInputScheme(std::make_unique<PijulInputScheme>());
});

} // namespace nix::fetchers
