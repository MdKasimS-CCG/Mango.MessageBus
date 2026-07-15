This package is required by Mango application to have Event Bus. It enables event-driven architecture for online system - Mango.

<h2>GitHub Packages Flow</h2>

        Developer
            │
            │Push
            ▼
        GitHub
            │
            ▼
        GitHub Action
            │
            ▼
        dotnet pack
            │
            ▼
        dotnet nuget push
            │
            ▼
        GitHub Packages
            │
            ▼
        AuthApi
        OrderApi
        ProductApi
        RewardApi
        EmailApi