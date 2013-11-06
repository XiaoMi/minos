class supervisor {
    include supervisor::deployer
    include supervisor::configurer
    include supervisor::starter
    include supervisor::monitor
}
