#include "fetchers.hh"
#include "store-api.hh"

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
                name != "url"sv && name != "channel"sv && name != "state"sv
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

        const Path tmpDir = createTempDir();
        const AutoDelete delTmpDir(tmpDir, true);
        const auto repoDir = tmpDir + "/source"sv;

        const auto url = parseURL(getStrAttr(input.attrs, "url"));
        const auto &repoUrl = url.base;
        const auto channel = maybeGetStrAttr(input.attrs, "channel");
        const auto state = maybeGetStrAttr(input.attrs, "state");

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
        deletePath(repoDir + "/.pijul"sv);

        auto storePath = store->addToStore(input.getName(), repoDir);

        return { std::move(storePath), input };
    }
};

static auto rPijulInputScheme = OnStartup([] {
    registerInputScheme(std::make_unique<PijulInputScheme>());
});

} // namespace nix::fetchers
