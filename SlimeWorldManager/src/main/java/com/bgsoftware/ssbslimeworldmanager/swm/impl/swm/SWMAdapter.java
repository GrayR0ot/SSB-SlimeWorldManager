package com.bgsoftware.ssbslimeworldmanager.swm.impl.swm;

import com.bgsoftware.ssbslimeworldmanager.api.DataSourceParams;
import com.bgsoftware.ssbslimeworldmanager.api.ISlimeAdapter;
import com.bgsoftware.ssbslimeworldmanager.api.ISlimeWorld;
import com.bgsoftware.ssbslimeworldmanager.api.SlimeUtils;
import com.bgsoftware.superiorskyblock.api.SuperiorSkyblock;
import com.bgsoftware.superiorskyblock.api.island.Island;
import com.bgsoftware.superiorskyblock.api.world.Dimension;
import com.google.common.base.Preconditions;
import com.infernalsuite.aswm.api.AdvancedSlimePaperAPI;
import com.infernalsuite.aswm.api.exceptions.CorruptedWorldException;
import com.infernalsuite.aswm.api.exceptions.NewerFormatException;
import com.infernalsuite.aswm.api.exceptions.SlimeException;
import com.infernalsuite.aswm.api.exceptions.UnknownWorldException;
import com.infernalsuite.aswm.api.loaders.SlimeLoader;
import com.infernalsuite.aswm.api.world.properties.SlimeProperties;
import com.infernalsuite.aswm.api.world.properties.SlimePropertyMap;
import com.infernalsuite.aswm.loaders.api.APILoader;
import com.infernalsuite.aswm.loaders.file.FileLoader;
import com.infernalsuite.aswm.loaders.mongo.MongoLoader;
import com.infernalsuite.aswm.loaders.mysql.MysqlLoader;
import org.bukkit.Bukkit;
import org.bukkit.World;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.logging.Level;

public class SWMAdapter implements ISlimeAdapter {

    private final SuperiorSkyblock plugin;
    private final AdvancedSlimePaperAPI aspAPI;
    private final SlimeLoader slimeLoader;

    public SWMAdapter(SuperiorSkyblock plugin, DataSourceParams dataSource) throws SlimeException {
        this.plugin = plugin;
        this.aspAPI = AdvancedSlimePaperAPI.instance();
        this.slimeLoader = this.getLoader(dataSource);
    }

    private SlimeLoader getLoader(DataSourceParams dataSourceParams) throws SlimeException {
        switch (dataSourceParams) {
            case DataSourceParams.MongoDB mongoDB -> {
                return new MongoLoader(mongoDB.database, mongoDB.collection, mongoDB.username, mongoDB.password, mongoDB.auth, mongoDB.host, mongoDB.port, mongoDB.url);
            }
            case DataSourceParams.File file -> {
                return new FileLoader(new File(file.path));
            }
            case DataSourceParams.MySQL mySQL -> {
                try {
                    return new MysqlLoader(mySQL.url, mySQL.host, mySQL.port, mySQL.database, mySQL.useSSL, mySQL.username, mySQL.password);
                } catch (SQLException ex) {
                    this.plugin.getLogger().warning("Unable to create SQLLoader: " + ex.getMessage());
                    throw new IllegalStateException(ex);
                }
            }
            case DataSourceParams.API api -> {
                return new APILoader(api.uri, api.username, api.token, api.ignoreSSLCertificate);
            }
            default -> throw new SlimeException("SlimeLoader not implemented for " + dataSourceParams);
        }
    }

    @Override
    public List<String> getSavedWorlds() throws IOException {
        return this.slimeLoader.listWorlds();
    }

    @Override
    public ISlimeWorld createOrLoadWorld(String worldName, World.Environment environment) {
        ISlimeWorld slimeWorld = SlimeUtils.getSlimeWorld(worldName);

        if (slimeWorld == null) {
            SlimePropertyMap properties = new SlimePropertyMap();

            try {
                if (this.slimeLoader.worldExists(worldName)) {
                    slimeWorld = new SWMSlimeWorld(this.aspAPI.readWorld(this.slimeLoader, worldName, false, properties));
                } else {
                    // set the default island properties accordingly
                    properties.setValue(SlimeProperties.DIFFICULTY, this.plugin.getSettings().getWorlds().getDifficulty().toLowerCase(Locale.ENGLISH));
                    properties.setValue(SlimeProperties.ENVIRONMENT, environment.name().toLowerCase(Locale.ENGLISH));

                    slimeWorld = new SWMSlimeWorld(this.aspAPI.createEmptyWorld(worldName, false, properties, this.slimeLoader));
                }

                SlimeUtils.setSlimeWorld(worldName, slimeWorld);
            } catch (IOException | CorruptedWorldException | NewerFormatException /*| WorldInUseException*/ |
                     UnknownWorldException ex) {
                this.plugin.getLogger().log(Level.SEVERE, "An exception occurred while trying to create or load world: " + worldName, ex);
            }
        }

        return slimeWorld;
    }

    @Override
    public void generateWorld(ISlimeWorld slimeWorld) {
        Preconditions.checkState(Bukkit.isPrimaryThread(), "cannot generate worlds async.");
        this.aspAPI.loadWorld(((SWMSlimeWorld) slimeWorld).getHandle(), true);
    }

    @Override
    public boolean deleteWorld(Island island, Dimension dimension) {
        String worldName = SlimeUtils.getWorldName(island.getUniqueId(), dimension);

        if (Bukkit.unloadWorld(worldName, false)) {
            Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
                try {
                    slimeLoader.deleteWorld(worldName);
                } catch (UnknownWorldException ignored) {
                    // World was not saved yet, who cares.
                } catch (IOException exception) {
                    throw new RuntimeException(exception);
                }
            });

            return true;
        }

        return false;
    }

}
