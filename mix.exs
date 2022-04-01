defmodule AfBi.MixProject do
  use Mix.Project

  def project do
    [
      app: :af_bi,
      version: "0.1.0",
      elixir: "~> 1.6",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      # extra_applications: [:logger]
        extra_applications: [
                        :logger,
                        :exprotobuf,
                        :eex,
                        :parse_trans,
                        :mix,
                        :connection,
                        :goth
                        ],
                         
      mod: {AfBi,[]}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    # [
    #   # {:dep_from_hexpm, "~> 0.3.0"},
    #   # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"},

    # ]
     [
       # json
      {:json, "~> 1.0.2"},
      
      # webserver
      {:cowboy, "~> 2.4.0"},

      # # releases
      {:distillery, "~> 1.5.2", runtime: false},

      # #aws
      {:ex_aws, "~> 2.0.2"},
      {:ex_aws_dynamo, "~> 2.0.0"},
      {:ex_aws_lambda, "~> 2.0.0"},
      {:ex_aws_s3, "~> 2.0.0"},
      {:ex_aws_sns, "~> 2.0.0"},
      {:ex_aws_ses, "~> 2.0.2"},

      # #An incredibly fast, pure Elixir JSON library
      {:poison, "~> 3.1.0"},

      # # simple HTTP client
      {:hackney, "~> 1.12.1"},

      # # A simple and fast CSV parsing and dumping library
      {:nimble_csv, "~> 0.4.0"},

      # #pb
      {:exprotobuf, "~> 1.2.9"},

      # #An sweet wrapper of :xmerl to help query xml docs
      {:sweet_xml, "~> 0.6.5"},

      # # smtp
      {:mailman, "~> 0.4.0"},
      
      {:plug, "~> 1.6.2"},

      {:logger_file_backend, "~> 0.0.10"},
      {:logstash_json, github: "kvine/logstash-json", branch: "modification"},
      {:flex_logger, "~> 0.2.1"},

      {:elx__slib, git: "https://github.com/kvine/elx__slib", branch: "master"} ,

      {:jose, "~> 1.11.1"},
      {:ojson, "~> 1.0.0"},
      {:csv, "~> 2.4.1"},
      {:timex, "~> 3.6.4"},

      {:google_api_big_query , "~> 0.0.1"},
      {:goth, "~> 0.7.0"}
    ]
  end
end
