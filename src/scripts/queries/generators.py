def traces(start_block, end_block) -> str:
    """
    Get the relevant columns from the 'traces' database to be used
    in the cairo_steps_script.
    """

    return  f"""
        select 
            tokenflow.decoded.traces.block_number,
            tokenflow.decoded.traces.trace_id,
            tokenflow.decoded.traces.tx_hash,
            tokenflow.decoded.traces.trace_type,
            tokenflow.decoded.traces.caller,
            tokenflow.decoded.traces.contract,
            tokenflow.decoded.traces.function,
            tokenflow.decoded.traces.execution_resources['n_steps'] as "STEPS"
        from tokenflow.decoded.traces
        where tokenflow.decoded.traces.block_number <= {end_block}
        and tokenflow.decoded.traces.block_number >= {start_block}
        and tokenflow.decoded.traces.chain_id = 'mainnet'
        """

def builtin_gas(start_block, end_block) -> str:
    """
    Create a table with columns 'block_number' and 'builtin_gas'
    where builtin_gas contains the total gas due to builtins in the block
    of number 'block_number'
    """

    return f""" 
    create or replace table builtin_gas_per_block as (
    with tx_and_builtins as (
    select
        *
    from tokenflow.decoded.transactions, lateral flatten( input => execution_resources['builtin_instance_counter'])
    where chain_id = 'mainnet'
    and block_number >= {start_block}
    and block_number <= {end_block}
    ),
    tx_and_builtins_v2 as (
    select
        block_number,
        case when tx_and_builtins.key = 'pedersen_builtin' then 0.32 * tx_and_builtins.value
        when tx_and_builtins.key = 'range_check_builtin' then 0.16 * tx_and_builtins.value
        when tx_and_builtins.key = 'bitwise_builtin' then 0.64 * tx_and_builtins.value
        when tx_and_builtins.key = 'poseidon_builtin' then 0.32 * tx_and_builtins.value
        when tx_and_builtins.key = 'ecdsa_builtin' then 20.48 * tx_and_builtins.value
        when tx_and_builtins.key = 'ec_op_builtin' then 10.24 * tx_and_builtins.value
        when tx_and_builtins.key = 'keccak_builtin' then 20.48 * tx_and_builtins.value
        end "BUILTIN_COST"
    from tx_and_builtins
    )
    select
        block_number,
        sum(tx_and_builtins_v2.builtin_cost) as "BUILTIN_GAS"
    from tx_and_builtins_v2
    group by block_number
    )
    ;
    """

def block_fee(start_block, end_block) -> str:
    """
    Create a table with columns 'block_number' and 'block_fee'
    where block_fee is the total actual fee paid in that block
    """
    return f""" 
    create or replace table block_fee as 
    select
        tokenflow.decoded.transactions.block_number,
        sum(tokenflow.decoded.transactions.actual_fee) as "BLOCK_FEE"
    from tokenflow.decoded.transactions
    where tokenflow.decoded.transactions.chain_id = 'mainnet'
    and tokenflow.decoded.transactions.block_number >= {start_block}
    and tokenflow.decoded.transactions.block_number <= {end_block}
    group by tokenflow.decoded.transactions.block_number
    order by tokenflow.decoded.transactions.block_number asc
    ;
    """

def steps_per_contract_per_block() -> str:
    """
    From the cairo_steps_script table deduce the steps per contract per block
    and create a table for it
    """

    return f""" 
    create or replace table steps_per_contract_per_block as (
    with main_query as (
        select
            cairo_steps_script.block_number,
            replace(cairo_steps_script.contract,'0x0','0x') as "CONTRACT", 
            sum(cairo_steps_script.individual_steps) as "STEPS_PER_CONTRACT"
        from cairo_steps_script
        group by cairo_steps_script.block_number, cairo_steps_script.contract
    )
    select 
        main_query.block_number,
        main_query.contract,
        main_query.steps_per_contract,
        sum(main_query.steps_per_contract) over (partition by main_query.block_number) as "STEPS_PER_BLOCK"
    from main_query
    order by main_query.block_number asc
    )
    ;
    """

def diffs_per_contract_per_block() -> str:
    """
    From the storage_diffs_script table deduce the diffs per contract per block
    and create a table for it
    """
    
    return f""" 
    create or replace table diffs_per_contract_per_block as (
    select
        storage_diffs_script.block_number,
        storage_diffs_script.contract,
        storage_diffs_script.updates_per_block as "DIFFS_PER_CONTRACT",
        sum(storage_diffs_script.updates_per_block) over (partition by storage_diffs_script.block_number) as "DIFFS_PER_BLOCK"
    from storage_diffs_script
    order by block_number asc
    )
    ;
    """

def join_steps_and_diffs() -> str:
    """
    Join the table steps_per_contract_per_block with the table
    diffs_per_contract_per_block
    """
    
    return f""" 
    create or replace table join_steps_and_diffs as (
    with temp_left as (
    select
        steps_per_contract_per_block.block_number,
        regexp_replace(steps_per_contract_per_block.contract,'0x0+','0x') as "CONTRACT",
        steps_per_contract_per_block.steps_per_contract,
        case when diffs_per_contract_per_block.diffs_per_contract is null then 0
        else diffs_per_contract_per_block.diffs_per_contract end as "DIFFS_PER_CONTRACT"
    from steps_per_contract_per_block
    left outer join diffs_per_contract_per_block on regexp_replace(steps_per_contract_per_block.contract,'0x0+','0x') = diffs_per_contract_per_block.contract
    and steps_per_contract_per_block.block_number = diffs_per_contract_per_block.block_number
    ),
    temp_right as (
    select
        diffs_per_contract_per_block.block_number,
        diffs_per_contract_per_block.contract,
        case when steps_per_contract_per_block.steps_per_contract is null then 0
        else steps_per_contract_per_block.steps_per_contract end as "STEPS_PER_CONTRACT",
        diffs_per_contract_per_block.diffs_per_contract
    from steps_per_contract_per_block
    right outer join diffs_per_contract_per_block on steps_per_contract_per_block.block_number = diffs_per_contract_per_block.block_number 
    and regexp_replace(steps_per_contract_per_block.contract,'0x0+','0x') = diffs_per_contract_per_block.contract
    )
    select
        *
    from temp_right
    union 
    select
        *
    from temp_left
    )
    ;    
    """

def final() -> str:
    """
    Join the tables
        - steps_per_contract_per_block
        - diffs_per_contract_per_block
        - block_fee
    """

    return f"""
    create or replace table final as (
        select
            join_steps_and_diffs.block_number,
            join_steps_and_diffs.contract,
            join_steps_and_diffs.steps_per_contract,
            sum(join_steps_and_diffs.steps_per_contract) over (partition by join_steps_and_diffs.block_number) as "STEPS_PER_BLOCK",
            diffs_per_contract,
            sum(join_steps_and_diffs.diffs_per_contract) over (partition by join_steps_and_diffs.block_number) as "DIFFS_PER_BLOCK",
            count(nullif(diffs_per_contract,0)) over (partition by join_steps_and_diffs.block_number) as "CONTRACTS_PER_BLOCK",
            block_fee.block_fee
        from join_steps_and_diffs
        inner join block_fee using (block_number)
    )
    ;
    """

def final_proportions(gas_per_step, gas_per_diff) -> str:
    """
    From the final table, creates a table with four additional columns:
        - 'FEE_PROPORTION_PER_BLOCK_STEPS': proportion (per block) of the total block fee which is due to steps and builtins
        - 'FEE_PROPORTION_PER_BLOCK_DIFFS': proportion (per block) of the total block fee which is due to storage_diffs
        - 'FEE_PROPORTION_PER_CONTRACT_DIFFS': proportion (per contract) of [total block fee due to steps and builtins], which is due to the contract
        - 'FEE_PROPORTION_PER_CONTRACT_STEPS': proportion (per contract) of [total block fee due to storage diffs], which is due to the contract
    """

    return f"""
    create or replace table final_proportions as (
    with temp_query as (
        select
            final.block_fee,
            final.block_number,
            final.contract,
            final.diffs_per_block,
            final.diffs_per_contract,
            final.steps_per_block,
            final.steps_per_contract,
            final.contracts_per_block,
            cast(builtin_gas_per_block.builtin_gas + final.steps_per_block * {gas_per_step} as float) / (builtin_gas_per_block.builtin_gas + final.steps_per_block * {gas_per_step} + final.diffs_per_block * {gas_per_diff} +  final.contracts_per_block * {gas_per_diff}) 
            as "FEE_PROPORTION_PER_BLOCK_STEPS",
            cast(final.diffs_per_block * {gas_per_diff} + final.contracts_per_block * {gas_per_diff} as float) / (builtin_gas_per_block.builtin_gas + final.steps_per_block * {gas_per_step} + final.diffs_per_block * {gas_per_diff} + final.contracts_per_block * {gas_per_diff}) 
            as "FEE_PROPORTION_PER_BLOCK_DIFFS"
        from final
        inner join builtin_gas_per_block using (block_number)
    )
    select
        *,
        case when temp_query.diffs_per_contract = 0 then 0
        else (temp_query.fee_proportion_per_block_diffs * (temp_query.diffs_per_contract + 1)) / (temp_query.diffs_per_block + temp_query.contracts_per_block)
        end "FEE_PROPORTION_PER_CONTRACT_DIFFS",
        (temp_query.fee_proportion_per_block_steps * temp_query.steps_per_contract) / temp_query.steps_per_block as "FEE_PROPORTION_PER_CONTRACT_STEPS"
    from temp_query
    )
    ;
    """


def final_fee_divided() -> str:
    """
    Multiplies the proportions computed in the final_proportions table
    with the block fee, thus obtaining the fee in wei per contract.
    """

    return f"""
    create or replace table final_fee_amounts_divided as (
    with temp_query as (
    select
        *,
        final_proportions.fee_proportion_per_contract_diffs * final_proportions.block_fee as "L1_FEE_PER_CONTRACT_PER_BLOCK",
        final_proportions.fee_proportion_per_contract_steps * final_proportions.block_fee as "L2_FEE_PER_CONTRACT_PER_BLOCK"
    from final_proportions
    )
    select
        temp_query.contract,
        sum(temp_query.l1_fee_per_contract_per_block) as "L1_FEE_PER_CONTRACT",
        sum(temp_query.l2_fee_per_contract_per_block) as "L2_FEE_PER_CONTRACT"
    from temp_query
    group by temp_query.contract
    )
    ;
    """


def ranking_l1_l2() -> str:
    """
    Aggregates the data in final_fee_amounts_divided across all blocks, per contract
    """
    
    return f""" 
    select top 10000
        contract,
        cast(l1_fee_per_contract as float) / pow(10,18) as "L1_FEE_PER_CONTRACT_ETH",
        cast(l2_fee_per_contract as float) / pow(10,18) as "L2_FEE_PER_CONTRACT_ETH",
        sum("L1_FEE_PER_CONTRACT_ETH" + "L2_FEE_PER_CONTRACT_ETH") as "FEE_PER_CONTRACT"
    from final_fee_amounts_divided
    group by contract, l1_fee_per_contract_eth, l2_fee_per_contract_eth
    order by fee_per_contract desc
    ;
    """
