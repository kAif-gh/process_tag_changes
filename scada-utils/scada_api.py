from fastapi import APIRouter, Depends, status, HTTPException, Request, Query, Response
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from fastapi_jwt_auth import AuthJWT

from sqlalchemy.orm import Session

from typing import List, Optional
from datetime import datetime
import pytz

from app.auth.auth import auth_check
from app.auth.auth_roles_const import READ_PER
from app.clients.tep_dgraph.kg_dgraph_client import KgDgraphClientGet
from app.core.logger import logger
from app.schemas.scada.scada_schemas import ScadaReferenceSignalSchema, ScadaAggReferenceSignalSchema
from app.api.dependencies import get_db
from app.core.config import settings, ScadaIntallationType, limiterSettings
from app.crud.scada.scada_signals_historicalvalue_crud import get_scada_historical_agg_signals
from app.clients.scada.kg_tepids_client import KgTepIdsGet
from app.core.circuitbreaker import ScadaCircuitBreaker
from slowapi import Limiter
from slowapi.util import get_remote_address

from app.clients.scada.utils.cache_scada_signals_helper import ScadaLocalCacheHelper
from .utils.scada_deps import (get_kg_tepids_client, ScadaRefSignalResponseBuilder,
                               check_and_sync_scada_cache_by_ref_name,
                               get_scada_latest_deserializer, get_tep_ids_by_ref_name,
                               get_scada_cache_latest_values_to_fetch_from, from_scada_agg_sig_values_to_item_values,
                               ScadaRefAggSignalResponseBuilder, check_and_sync_scada_cache_by_ref_names,
                               get_tep_ids_by_ref_names, get_tep_ids_by_ref_names_tbr_ids, check_response_data,
                               check_retention, parse_datetime)
from app.api.api_v1.api_deps import (end_date_back_hours_constraints, start_datetime_calculation,
                                     get_last_values_from_cache, get_kg_dgraph_client, get_request_flow,
                                     get_asset_shortname, is_doggerbank_prod, check_installation_type_endpoint_params,
                                     get_measurement_std_ref_names)

router = APIRouter(
    prefix="",
    tags=["scada"]
)

breaker = ScadaCircuitBreaker()
limiter = Limiter(key_func=get_remote_address)

signal_deserializer = get_scada_latest_deserializer()

cache_helper = ScadaLocalCacheHelper(settings.SCADA.CACHE.SIZE, settings.SCADA.CACHE.TTL_MAX)

@router.get(
    "/ts/scada-reference/latest/{scada_reference_signal_name}",
    description="Get latest time series data by SCADA Reference signal name",
    operation_id="lastScadaReferenceSignal",
    status_code=200,
    response_model=List[ScadaReferenceSignalSchema],
)
@breaker
@limiter.limit(limiterSettings.SCADA_LIMITS)
def get_scada_signals_latest_states_by_scada_reference_signal(
        scada_reference_signal_name: str,
        offshore_wind_farm_id: str,
        request: Request,
        offshore_wind_turbine_id: Optional[str] = None,
        kg_tepids_client: KgTepIdsGet = Depends(get_kg_tepids_client),
        kg_dgraph_client: KgDgraphClientGet = Depends(get_kg_dgraph_client),
        authorize: AuthJWT = Depends(),
):
    flow_type = get_request_flow(request)

    auth_check(authorize, [READ_PER], flow_type=flow_type)

    check_and_sync_scada_cache_by_ref_name(wf_id=offshore_wind_farm_id, ref_sig_name=scada_reference_signal_name,
                                           kg_tepids_client=kg_tepids_client, kg_dgraph_client=kg_dgraph_client,
                                           authorize=authorize, flow_type=flow_type, cache_helper=cache_helper)

    tep_ids = get_tep_ids_by_ref_name(wf_id=offshore_wind_farm_id, ref_sig_name=scada_reference_signal_name,
                                      tbr_id=offshore_wind_turbine_id, cache_helper=cache_helper)

    cache_values_to_fetch_from = get_scada_cache_latest_values_to_fetch_from()

    last_values = get_last_values_from_cache(tep_ids=tep_ids, cache_item_values=cache_values_to_fetch_from,
                                             item_deserializer=signal_deserializer)
    if not last_values:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No Scada signals found for latest values")

    last_values_sorted = sorted(last_values, key=lambda s: s.tag, reverse=False)

    # specific case for doggerbank prod
    is_db_prod = is_doggerbank_prod(request)

    result: List[ScadaReferenceSignalSchema] = ScadaRefSignalResponseBuilder(
        given_ref_name=scada_reference_signal_name,
        wf_id=offshore_wind_farm_id,
        tbr_id=offshore_wind_turbine_id,
        scada_sig_values=last_values_sorted,
        cache_helper=cache_helper,
        is_doggerbank_prod=is_db_prod
    ).build()

    response_body = jsonable_encoder(result)
    return JSONResponse(status_code=status.HTTP_200_OK, content=response_body)

@router.get(
    "/ts/scada-measurement-standard-name/latest/{measurement_standard_name}",
    description="Get latest time series data by measurement standard name",
    operation_id="lastScadaSignalByMeasStdName",
    status_code=200,
    response_model=List[ScadaReferenceSignalSchema],
)
@breaker
@limiter.limit(limiterSettings.SCADA_LIMITS)
def get_scada_signals_latest_states_by_measurement_standard_name(
        measurement_standard_name: str,
        offshore_wind_farm_id: str,
        request: Request,
        offshore_wind_turbine_id: Optional[str] = None,
        kg_tepids_client: KgTepIdsGet = Depends(get_kg_tepids_client),
        kg_dgraph_client: KgDgraphClientGet = Depends(get_kg_dgraph_client),
        authorize: AuthJWT = Depends(),
):
    flow_type = get_request_flow(request)
    auth_check(authorize, [READ_PER], flow_type=flow_type)

    asset_short_name = get_asset_shortname(request)
    ref_names = get_measurement_std_ref_names(asset_short_name=asset_short_name,
                                              measurement_standard_name=measurement_standard_name)

    if not ref_names:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Measurement standard name {measurement_standard_name} "
                                   f"is not supported yet, Please contact TEP Team")

    check_and_sync_scada_cache_by_ref_names(wf_id=offshore_wind_farm_id, ref_sig_names=set(ref_names),
                                            kg_tepids_client=kg_tepids_client, kg_dgraph_client=kg_dgraph_client,
                                            authorize=authorize, flow_type=flow_type, cache_helper=cache_helper)

    tep_ids = get_tep_ids_by_ref_names(wf_id=offshore_wind_farm_id, ref_sig_names=set(ref_names),
                                       tbr_id=offshore_wind_turbine_id, cache_helper=cache_helper)

    cache_values_to_fetch_from = get_scada_cache_latest_values_to_fetch_from()

    last_values = get_last_values_from_cache(tep_ids=tep_ids, cache_item_values=cache_values_to_fetch_from,
                                             item_deserializer=signal_deserializer)
    if not last_values:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No Scada signals found for latest values")

    last_values_sorted = sorted(last_values, key=lambda s: s.tag, reverse=False)

    # specific case for doggerbank prod
    is_db_prod = is_doggerbank_prod(request)

    result = []
    for ref_name in ref_names:
        result_by_ref_name: List[ScadaReferenceSignalSchema] = ScadaRefSignalResponseBuilder(
            given_ref_name=ref_name,
            wf_id=offshore_wind_farm_id,
            tbr_id=offshore_wind_turbine_id,
            scada_sig_values=last_values_sorted,
            cache_helper=cache_helper,
            is_doggerbank_prod=is_db_prod
        ).build()
        result.extend(result_by_ref_name)

    response_body = jsonable_encoder(result)
    return JSONResponse(status_code=status.HTTP_200_OK, content=response_body)


active_installation_type = settings.STORM_EP_INSTL_TYPE_ACTIVE

if active_installation_type:
    @router.get(
        "/ts/scada-installation-type/latest/{installation_type}",
        description="Get latest time series data by SCADA installationtype",
        operation_id="lastScadaInstallationTypeSignal",
        status_code=200,
        response_model=List[ScadaReferenceSignalSchema],
    )
    @breaker
    @limiter.limit(limiterSettings.SCADA_LIMITS)
    def get_scada_signals_latest_states_by_installation_type(
            installation_type: ScadaIntallationType,
            offshore_wind_farm_id: str,
            request: Request,
            scada_reference_signal_name: str = Query(None,
                                                     description=f"Required for "
                                                                 f"{ScadaIntallationType.offshore_wind_turbine} "
                                                                 f"installation type only"),
            measurement_standard_name: str = Query(None,
                                                   description=f"Required for "
                                                               f"{ScadaIntallationType.offshore_wind_turbine} "
                                                               f"installation type only"),
            offshore_wind_turbine_ids: Optional[List[str]] = Query(None,
                                                                   description=f"Optional for {ScadaIntallationType.offshore_wind_turbine} "
                                                                               f"installation type only", max_items=5),
            scada_signal_names: Optional[List[str]] = Query(None,
                                                            description=f"Scada signal names", max_items=5),
            kg_tepids_client: KgTepIdsGet = Depends(get_kg_tepids_client),
            kg_dgraph_client: KgDgraphClientGet = Depends(get_kg_dgraph_client),
            authorize: AuthJWT = Depends(),
    ):
        flow_type = get_request_flow(request)

        check_installation_type_endpoint_params(flow_type=flow_type, installation_type=installation_type,
                                                scada_reference_signal_name=scada_reference_signal_name,
                                                measurement_standard_name=measurement_standard_name,
                                                scada_signal_names=scada_signal_names,
                                                offshore_wind_turbine_ids=offshore_wind_turbine_ids,
                                                is_latest_ep=True)

        auth_check(authorize, [READ_PER], flow_type=flow_type)

        # ref names in function of the installation type and the wind farm
        ref_names = []
        force_ref_name = False
        if installation_type == ScadaIntallationType.offshore_wind_turbine:
            if scada_reference_signal_name is not None:
                ref_names = [str(scada_reference_signal_name)]
            elif measurement_standard_name is not None:
                asset_short_name = get_asset_shortname(request)
                ref_names = get_measurement_std_ref_names(asset_short_name=asset_short_name,
                                                          measurement_standard_name=measurement_standard_name)
                if not ref_names:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                        detail=f"Measurement standard name {measurement_standard_name} "
                                               f"is not supported yet, Please contact TEP Team")
            if scada_signal_names is not None:
                ref_names = [str(installation_type.value)]
                force_ref_name = True

        if installation_type in [ScadaIntallationType.onshore_substation, ScadaIntallationType.offshore_substation]:
            ref_names = [str(installation_type.value)]
            force_ref_name = True

        check_and_sync_scada_cache_by_ref_names(wf_id=offshore_wind_farm_id, ref_sig_names=set(ref_names),
                                                kg_tepids_client=kg_tepids_client, kg_dgraph_client=kg_dgraph_client,
                                                authorize=authorize, flow_type=flow_type, cache_helper=cache_helper,
                                                force_ref_name=force_ref_name)

        tep_ids = set()
        if scada_signal_names is not None:
            scada_signal_names = list(set(scada_signal_names))
            for sig_name in scada_signal_names:
                sig_tup = cache_helper.get_tep_id_tbr_by_sig_name(wf_id=offshore_wind_farm_id,
                                                                  given_ref_name=ref_names[0], sig_name=sig_name)
                if sig_tup is not None and len(sig_tup) > 0 and sig_tup[1] is not None and len(sig_tup[1]) > 0:
                    tep_ids.add(sig_tup[1])
        else:
            turbine_ids = set(offshore_wind_turbine_ids) if offshore_wind_turbine_ids is not None else None
            tep_ids = get_tep_ids_by_ref_names_tbr_ids(wf_id=offshore_wind_farm_id, ref_sig_names=set(ref_names),
                                                       tbr_ids=turbine_ids,
                                                       cache_helper=cache_helper)

        cache_values_to_fetch_from = get_scada_cache_latest_values_to_fetch_from()

        last_values = get_last_values_from_cache(tep_ids=tep_ids, cache_item_values=cache_values_to_fetch_from,
                                                 item_deserializer=signal_deserializer)
        if not last_values:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail="No Scada signals found for latest values")

        last_values_sorted = sorted(last_values, key=lambda s: s.tag, reverse=False)

        is_db_prod = False
        # specific case for doggerbank prod, only apply to wtb
        if installation_type == ScadaIntallationType.offshore_wind_turbine:
            is_db_prod = is_doggerbank_prod(request)

        result = []
        for ref_name in ref_names:
            result_by_ref_name: List[ScadaReferenceSignalSchema] = ScadaRefSignalResponseBuilder(
                given_ref_name=ref_name,
                wf_id=offshore_wind_farm_id,
                tbr_id=None,
                scada_sig_values=last_values_sorted,
                cache_helper=cache_helper,
                is_doggerbank_prod=is_db_prod
            ).build()
            result.extend(result_by_ref_name)

        response_body = jsonable_encoder(result)
        return JSONResponse(status_code=status.HTTP_200_OK, content=response_body)


@router.get(
    "/ts/scada-reference/historical/{scada_reference_signal_name}",
    description="Get historical SCADA aggregated reference signals ",
    operation_id="historicalScadaAggReferenceSignals",
    status_code=status.HTTP_200_OK,
    response_model=List[ScadaAggReferenceSignalSchema],
)
@breaker
@limiter.limit(limiterSettings.SCADA_LIMITS)
def get_scada_reference_historical_agg_signals(
        scada_reference_signal_name: str,
        offshore_wind_farm_id: str,
        end_datetime: datetime,
        hours_back: int,
        request: Request,
        offshore_wind_turbine_id: Optional[str] = None,
        db: Session = Depends(get_db),
        kg_tepids_client: KgTepIdsGet = Depends(get_kg_tepids_client),
        kg_dgraph_client: KgDgraphClientGet = Depends(get_kg_dgraph_client),
        authorize: AuthJWT = Depends(),
):
    flow_type = get_request_flow(request)
    auth_check(authorize, [READ_PER], flow_type=flow_type)

    max_hours_back = settings.SCADA.MAX_HOURS_BACK

    # Parse end_datetime
    end_datetime = parse_datetime(end_datetime)
    logger.debug(f"endTime after parsing: {end_datetime}")

    # check date and hours back
    end_date_back_hours_constraints(end_datetime, hours_back, max_hours_back)
    # calculate start_datetime
    start_datetime = start_datetime_calculation(end_datetime, hours_back)

    check_and_sync_scada_cache_by_ref_name(wf_id=offshore_wind_farm_id, ref_sig_name=scada_reference_signal_name,
                                           kg_tepids_client=kg_tepids_client, kg_dgraph_client=kg_dgraph_client,
                                           authorize=authorize, flow_type=flow_type, cache_helper=cache_helper)

    tep_ids = get_tep_ids_by_ref_name(wf_id=offshore_wind_farm_id, ref_sig_name=scada_reference_signal_name,
                                      tbr_id=offshore_wind_turbine_id, cache_helper=cache_helper)

    check_retention(end_datetime, hours_back)

    historical_agg_values = get_scada_historical_agg_signals(
        db=db, tep_ids=tep_ids, start_datetime=start_datetime, end_datetime=end_datetime)

    if not historical_agg_values:
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    item_agg_values = from_scada_agg_sig_values_to_item_values(historical_agg_values)

    # specific case for doggerbank prod
    is_db_prod = is_doggerbank_prod(request)

    result: List[ScadaAggReferenceSignalSchema] = ScadaRefAggSignalResponseBuilder(
        given_ref_name=scada_reference_signal_name,
        wf_id=offshore_wind_farm_id,
        tbr_id=offshore_wind_turbine_id,
        scada_agg_sig_values=item_agg_values,
        cache_helper=cache_helper,
        is_doggerbank_prod=is_db_prod
    ).build()

    check_response_data(result)

    response_body = jsonable_encoder(result)
    return JSONResponse(status_code=status.HTTP_200_OK, content=response_body)


if active_installation_type:
    @router.get(
        "/ts/scada-installation-type/historical/{installation_type}",
        description="Get historical aggregated time series data by SCADA installation type",
        operation_id="historicalScadaAggDataInstallationTypeSignal",
        status_code=status.HTTP_200_OK,
        response_model=List[ScadaAggReferenceSignalSchema],
    )
    @breaker
    @limiter.limit(limiterSettings.SCADA_LIMITS)
    def get_scada_historical_agg_signals_by_installation_type(
            installation_type: ScadaIntallationType,
            offshore_wind_farm_id: str,
            end_datetime: datetime,
            hours_back: int,
            request: Request,
            scada_reference_signal_name: str = Query(None,
                                                     description=f"Required for "
                                                                 f"{ScadaIntallationType.offshore_wind_turbine} "
                                                                 f"installation type only"),
            measurement_standard_name: str = Query(None,
                                                   description=f"Required for "
                                                               f"{ScadaIntallationType.offshore_wind_turbine} "
                                                               f"installation type only"),
            offshore_wind_turbine_ids: Optional[List[str]] = Query(None,
                                                                   description=f"Optional for {ScadaIntallationType.offshore_wind_turbine} "
                                                                               f"installation type only"),
            scada_signal_names: Optional[List[str]] = Query(None,
                                                            description=f"Scada signal names",
                                                            max_items=5),
            db: Session = Depends(get_db),
            kg_tepids_client: KgTepIdsGet = Depends(get_kg_tepids_client),
            kg_dgraph_client: KgDgraphClientGet = Depends(get_kg_dgraph_client),
            authorize: AuthJWT = Depends(),
    ):
        flow_type = get_request_flow(request)

        check_installation_type_endpoint_params(flow_type=flow_type, installation_type=installation_type,
                                                scada_reference_signal_name=scada_reference_signal_name,
                                                measurement_standard_name=measurement_standard_name,
                                                scada_signal_names=scada_signal_names,
                                                offshore_wind_turbine_ids=offshore_wind_turbine_ids)

        max_hours_back = settings.SCADA.MAX_HOURS_BACK

        # Parse end_datetime
        end_datetime = parse_datetime(end_datetime)
        logger.debug(f"endTime after parsing: {end_datetime}")

        # check date and hours back
        end_date_back_hours_constraints(end_datetime, hours_back, max_hours_back)
        # calculate start_datetime
        start_datetime = start_datetime_calculation(end_datetime, hours_back)

        auth_check(authorize, [READ_PER], flow_type=flow_type)

        # ref names in function of the installation type and the wind farm
        ref_names = []
        force_ref_name = False
        if installation_type == ScadaIntallationType.offshore_wind_turbine:
            if scada_reference_signal_name is not None:
                ref_names = [str(scada_reference_signal_name)]
            elif measurement_standard_name is not None:
                asset_short_name = get_asset_shortname(request)
                ref_names = get_measurement_std_ref_names(asset_short_name=asset_short_name,
                                                          measurement_standard_name=measurement_standard_name)
                if not ref_names:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                        detail=f"Measurement standard name {measurement_standard_name} "
                                               f"is not supported yet, Please contact TEP Team")
            if scada_signal_names is not None:
                ref_names = [str(installation_type.value)]
                force_ref_name = True

        if installation_type in [ScadaIntallationType.onshore_substation, ScadaIntallationType.offshore_substation]:
            ref_names = [str(installation_type.value)]
            force_ref_name = True

        check_and_sync_scada_cache_by_ref_names(wf_id=offshore_wind_farm_id, ref_sig_names=set(ref_names),
                                                kg_tepids_client=kg_tepids_client, kg_dgraph_client=kg_dgraph_client,
                                                authorize=authorize, flow_type=flow_type, cache_helper=cache_helper,
                                                force_ref_name=force_ref_name)

        tep_ids = set()
        if scada_signal_names is not None:
            scada_signal_names = list(set(scada_signal_names))
            for sig_name in scada_signal_names:
                sig_tup = cache_helper.get_tep_id_tbr_by_sig_name(wf_id=offshore_wind_farm_id,
                                                                  given_ref_name=ref_names[0], sig_name=sig_name,
                                                                  is_agg=True)
                if sig_tup is not None and len(sig_tup) > 0 and sig_tup[1] is not None and len(sig_tup[1]) > 0:
                    tep_ids.add(sig_tup[1])
        else:
            turbine_ids = set(offshore_wind_turbine_ids) if offshore_wind_turbine_ids is not None else None
            tep_ids = get_tep_ids_by_ref_names_tbr_ids(wf_id=offshore_wind_farm_id, ref_sig_names=set(ref_names),
                                                       tbr_ids=turbine_ids,
                                                       cache_helper=cache_helper, is_agg=True)

        check_retention(end_datetime, hours_back)
        historical_agg_values = get_scada_historical_agg_signals(
            db=db, tep_ids=tep_ids, start_datetime=start_datetime, end_datetime=end_datetime)
        if not historical_agg_values:
            return Response(status_code=status.HTTP_204_NO_CONTENT)

        item_agg_values = from_scada_agg_sig_values_to_item_values(historical_agg_values)
        is_db_prod = False
        # specific case for doggerbank prod, only apply to wtb
        if installation_type == ScadaIntallationType.offshore_wind_turbine:
            is_db_prod = is_doggerbank_prod(request)

        result = []
        for ref_name in ref_names:
            result_by_ref_name: List[ScadaAggReferenceSignalSchema] = ScadaRefAggSignalResponseBuilder(
                given_ref_name=ref_name,
                wf_id=offshore_wind_farm_id,
                tbr_id=None,
                scada_agg_sig_values=item_agg_values,
                cache_helper=cache_helper,
                is_doggerbank_prod=is_db_prod
            ).build()
            result.extend(result_by_ref_name)

        check_response_data(result)

        response_body = jsonable_encoder(result)
        return JSONResponse(status_code=status.HTTP_200_OK, content=response_body)